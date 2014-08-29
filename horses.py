from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
import backoff
import uuid
import re
import trueskill

root = "http://www.racingpost.com"
epoch = datetime(2000, 1, 1)

@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
@backoff.on_exception(backoff.expo, requests.exceptions.ConnectionError, max_tries=5)
def get_url(url):
	return requests.get(url)

def date_or_none(current_pointer):
	if current_pointer == None:
		return None
	else:
		return current_pointer["value"]

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def extract_race_id(url):
	m = re.search('race_id=(\d+)', url)
	if m == None:
		return None, None
	else:
		return m.group(1), url

def process_race_day(status):
	db = MongoClient().racing
	for race in db.race_day.find({"status": status}).batch_size(500):
		r = get_url(root + race["race_url"])
		if r.status_code == 200:
			guid = uuid.uuid1()
			download_uri = "/Users/waynequinlivan/.racingcache/{0}".format(guid)
			soup = BeautifulSoup(r.text)
			html = soup.select("div.popUp")
			if html == None or len(html) == 0:
				race["status"] = "error"
				race["error_message"] = "broken html"
			else:
				fo = open("/Users/waynequinlivan/.racingcache/{0}".format(guid), "wb")
				fo.write(str(html[0]))
				fo.close()
				race["status"] = "downloaded"
				race["download_uri"] = download_uri
			db.race_day.save(race)

def redo_errors():
	process_race_day("error")

def download_race_links():
	process_race_day("new")

def get_race_links():
	db = MongoClient().racing
	current_pointer = db.current_pointer_collection.find_one({"type": "race_day"})
	for d in daterange(date_or_none(current_pointer) or epoch, datetime.now()):
		r = get_url("http://www.racingpost.com/horses2/results/home.sd?r_date={0}".format(d.strftime("%Y-%m-%d")))
		if r.status_code == 200:
			soup = BeautifulSoup(r.text)
			links = soup.select("ul.activeLink a")
			if links == None:
				db.race_day_errors.insert({"date": d, "url": r.url})
			else:
				for l in links:
					race_id, href = extract_race_id(l.get('href').replace('amp;','').replace('&popup=yes',''))
					db.race_day.update({"race_id": race_id}, {"race_id": race_id, "race_url": href, "race_date": d, "status": "new"}, upsert=True)
				db.current_pointer_collection.update({"type": "race_day"}, {"type": "race_day", "value": d}, upsert=True)

def extract_horses():
	db = MongoClient().racing
	for race in db.race_day.find({"status": "downloaded", "horses_extracted": { "$exists": False }}).batch_size(500):
		uri = race["download_uri"]
		fo = open(uri, "r")
		soup = BeautifulSoup(fo.read())
		for tr in soup.select("table.resultRaceGrid tbody"):
			for race_items in tr.select("tr:nth-of-type(2)"):
				hid = race_items["data-hid"].strip()
				if db.horses.find({"hid": hid}).limit(1).count() == 0:
					try:
						name = race_items.select("td:nth-of-type(4) span b a")[0].text.strip()
						db.horses.save({"hid": hid, "name": name})
					except:
						db.horses_error.save({"message": sys.exc_info()[0], "race_day_id": race["_id"]})
		fo.close()
		race["horses_extracted"] = True
		db.race_day.save(race)
				
def extract_trainer_id(url):
	m = re.search('trainer_id=(\d+)', url)
	if m == None:
		return None
	else:
		return m.group(1)

def extract_headgear(result, value):
	if "b" in value:
		result["blinkers"] = True
	if "v" in value:
		result["visor"] = True
	if "h" in value:
		result["hood"] = True
	if "e/s" in value:
		result["eye_shield"] = True
	if "t" in value:
		result["tongue_strap"] = True
	if "p" in value:
		result["sheepskin_cheekpieces"] = True

def extract_impediments(content):
	weight = {}
	headgear = {}
	count = 0
	for value in content.stripped_strings:
		count += 1
		if count == 1:
			m = re.search('(\d+)-(\d+)', value)
			if m != None:
				weight["st"] = m.group(1)
				weight["lb"] = m.group(2)
		if count == 2:
			if content.img != None:
				weight["overweight"] = value
			else:
				extract_headgear(headgear, value)
		if count == 3:
			if content.img != None:
				extract_headgear(headgear, value)
			else:
				if value == "1":
					headgear["first_time"] = True
		if count == 4 and content.img != None:
			if value == "1":
				headgear["first_time"] = True

	return weight, headgear

def extract_results():
	db = MongoClient().racing
	for race in db.race_day.find({"status": "downloaded", "results_extracted": { "$exists": False }}).batch_size(500):
		uri = race["download_uri"]
		fo = open(uri, "r")
		soup = BeautifulSoup(fo.read())
		runners = []
		for tr in soup.select("table.resultRaceGrid tbody"):
			for race_items in tr.select("tr:nth-of-type(2)"):
				runner = {}
				hid = race_items["data-hid"].strip()
				position = race_items.select("td:nth-of-type(2) h3")[0].text.strip()
				dist = race_items.select("td:nth-of-type(3)")[0].text.strip()
				age = race_items.select("td:nth-of-type(5)")[0].text.strip()
				weight, headgear = extract_impediments(race_items.select("td:nth-of-type(6)")[0])
				tid = extract_trainer_id(race_items.select("td:nth-of-type(7) a")[0].get("href").strip())
				runner["hid"] = hid
				runner["position"] = position
				if dist: runner["dist"] = dist
				runner["age"] = age
				runner["weight"] = weight
				if headgear: runner["headgear"] = headgear
				runner["tid"] = tid
				runners.append(runner)
		race["results"] = runners
		race["results_extracted"] = True
		db.race_day.save(race)
		fo.close()

def is_eligible_for_rating(runner):
	return runner["position"].isdigit()

def score_horses():
	db = MongoClient().racing
	query = { 
	           "status": "downloaded", 
	           "results_extracted": { "$exists": True },
	           "ratings_calculated": { "$exists": False },
	           "$where": "this.results.length > 4"
	        }
	env = trueskill.TrueSkill()
	for race in db.race_day.find(query).batch_size(30):
		rating_groups = []
		eligible_runners = []
		for runner in race["results"]:
			if is_eligible_for_rating(runner):
				eligible_runners.append(runner)
		if len(eligible_runners) >= 2:
			for runner in eligible_runners:
				ratings = db.ratings.find({"hid": runner["hid"]}).sort("date", -1).limit(1)
				if ratings.count() == 0:
					rating_groups.append((env.create_rating(),))
				else:
					rating = ratings.next()
					rating_groups.append((env.create_rating(rating["mu"], rating["sigma"]),))
			rated_rating_groups = env.rate(rating_groups)
			for runner, rating in zip(eligible_runners, rated_rating_groups):
				(r,) = rating
				db.ratings.save({"hid": runner["hid"], "date": race["race_date"], "mu": r.mu, "sigma": r.sigma})
		race["ratings_calculated"] = True
		db.race_day.save(race)
