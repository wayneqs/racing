from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
import backoff
import uuid
import re

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
	for race in db.race_day.find({"status": status}):
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


