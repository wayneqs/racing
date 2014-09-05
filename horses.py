#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
import backoff
import uuid
import re
import trueskill
import math
import numpy
import pandas
import itertools
import sys
import dataextractors

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

def process_race_day(status):
	db = MongoClient().racing
	for race in db.races.find({"status": status}).batch_size(500):
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
			db.races.save(race)

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
				db.races_errors.insert({"date": d, "url": r.url})
			else:
				for l in links:
					race_id, href = extract_race_id(l.get('href').replace('amp;','').replace('&popup=yes',''))
					db.races.update({"race_id": race_id}, {"race_id": race_id, "race_url": href, "race_date": d, "status": "new"}, upsert=True)
				db.current_pointer_collection.update({"type": "race_day"}, {"type": "race_day", "value": d}, upsert=True)

def extract_race_id(url):
	m = re.search('race_id=(\d+)', url)
	if m == None:
		return None, None
	else:
		return m.group(1), url

def extract_horses():
	db = MongoClient().racing
	for race in db.races.find({"status": "downloaded", "data_extracted": { "$exists": False }}).batch_size(500):
		uri = race["download_uri"]
		fo = open(uri, "r")
		soup = BeautifulSoup(fo.read())
		for tr in soup.select("table.resultRaceGrid tbody"):
			for race_items in tr.select("tr:nth-of-type(2)"):
				hid = race_items["data-hid"].strip()
				if db.horses.find_one({"hid": hid}) == None:
					try:
						name = race_items.select("td:nth-of-type(4) span b a")[0].text.strip()
						db.horses.save({"hid": hid, "name": name})
					except:
						db.horses_error.save({"message": sys.exc_info()[0], "race_day_id": race["_id"]})
		fo.close()
		race["horses_extracted"] = True
		db.races.save(race)
				
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

def extract_runner_distance(distance_string):
	distance = 0
	if distance_string != None:
		fractions = {u'\xbd': 0.5, u'\xbc': 0.25, u'\xbe': 0.75, 
					'nk': 0.35, 'snk': 0.25, 'hd': 0.22, 'shd': 0.18, 
					'dht': 0, 'nse': 0.14, 'dist': 50, 'min': 0.8}
		(whole, fractional) = re.match('(\d+)?(\D+)?', distance_string).groups()
		if whole == None and fractional == None:
			return 0
		if whole != None:
			distance += int(whole)
		if fractional != None:
			distance += fractions[fractional.replace("fs", "")]
	return distance * 8 * 0.33333

def extract_race_time(db, race, soup):
	if race.get("winning_time_secs") == None:
		race_info = soup.select("div.raceInfo")
		if len(race_info) == 0:
			race["error_status"] = "no_race_info"
			db.race_info_errors.save(race)
		else:
			if "TIME" not in race_info[0].text:
				race["error_status"] = "no_time"
				race["race_info"] = race_info[0].text
				db.race_info_errors.save(race)
			else:
				m = re.findall('TIME ([0-9]*\.?[0-9]+)s|TIME (\d+)m ([0-9]*\.?[0-9]+)s', race_info[0].text)
				if len(m) == 0 or all(x == "" for x in m[0]):
					race["error_status"] = "time_format"
					race["race_info"] = race_info[0].text
					db.race_info_errors.save(race)
				else:
					matches = m[0]
					try:
						if matches[0] != "":
							secs = float(matches[0])
							if secs > 0.0:
								race["winning_time_secs"] = secs
						else:
							secs = float(matches[1]) * 60 + float(matches[2])
							race["winning_time_secs"] = secs
						return race
					except ValueError, e:
						race["error_status"] = "time_format"
						race["race_info"] = race_info[0].text
						db.race_info_errors.save(race)
	return None

def extract_data():
	db = MongoClient().racing
	for race in db.races.find({"status": "downloaded", "data_extracted": { "$exists": False }}).batch_size(500):
		uri = race["download_uri"]
		fo = open(uri, "r")
		soup = BeautifulSoup(fo.read())
		extract_race_time(db, race, soup)
		runners, errors = dataextractors.extract_runners(soup)
		race["results"] = runners
		race["data_extracted"] = True
		db.races.save(race)
		fo.close()

def normalize_speed(dist, time):
	standard_distance = 1000
	standard_ratio = float(standard_distance) / float(dist)
	time2 = time * math.pow(standard_ratio, 1.06)
	return standard_distance / time2

def calculate_dist_cat(dist):
	if race_dist <= short:
		return "s"
	elif race_dist <= medium:
		return "m"
	else:
		return "l"

def generate_moving_averages(horse_id, dates, values, period, label):
	for date, value in zip(dates, values):
		yield {"date": date, "period": period, "hid": horse_id, "value": value, "label": label}

def calculate_moving_averages(speeds, horse_id=None):
	if horse_id == None:
		for horse_id, speeds in itertools.groupby(speeds, lambda speed: speed["hid"]):
			calculate_moving_averages(speeds, horse_id)
	else:
		normalized_speeds = []
		raw_speeds = []
		dates = []
		for speed in sorted(speeds, key=lambda d: d["date"]):
			normalized_speeds.append(speed["normalized_speed"])
			raw_speeds.append(speed["speed"])
			dates.append(speed["date"])
		dn = pandas.Series(normalized_speeds, dates)
		d = pandas.Series(raw_speeds, dates)

		dn_mva3 = pandas.rolling_mean(dn, 3)
		dn_mva5 = pandas.rolling_mean(dn, 5)
		dn_mva8 = pandas.rolling_mean(dn, 8)
		d_mva3 = pandas.rolling_mean(d, 3)
		d_mva5 = pandas.rolling_mean(d, 5)
		d_mva8 = pandas.rolling_mean(d, 8)
		zipped = zip(generate_moving_averages(horse_id, dates, dn_mva3, 3, "normalized"),
				     generate_moving_averages(horse_id, dates, dn_mva5, 5, "normalized"),
					 generate_moving_averages(horse_id, dates, dn_mva8, 8, "normalized"),
					 generate_moving_averages(horse_id, dates, d_mva3, 3, "raw"),
					 generate_moving_averages(horse_id, dates, d_mva5, 5, "raw"),
					 generate_moving_averages(horse_id, dates, d_mva8, 8, "raw"))
		return list(itertools.chain.from_iterable(zipped))

def is_finisher(runner):
	return runner["position"].isdigit()

def get_rating(db, cache, hid):
	from_cache  = cache.get(hid)
	if from_cache != None:
		return from_cache
	else:
		from_db = db.unbiased_ratings.find_one({"hid": hid}, sort=[("date", -1)])
		if from_db != None:
			rating = from_db.next()
			cache[hid] = rating
			return rating
	return None

def save_rating(db, cache, rating):
	cache[rating["hid"]] = rating
	db.unbiased_ratings.save(rating)

def warm_cache(db, cache):
	print "warming cache"
	d = timedelta(weeks=52)
	most_recent_rating = db.unbiased_ratings.find_one(sort=[("date", -1)])
	print most_recent_rating
	if most_recent_rating != None:
		start = most_recent_rating["date"] - d
		ratings = db.unbiased_ratings.find({"date": { "$gt": start}}).sort("date", 1)
		print "adding {0} ratings to the cache".format(ratings.count())
		for rating in ratings:
			cache[rating["hid"]] = rating
	print "cache warmed"

def score_horses():
	db = MongoClient().racing
	cache = {}
	query = { 
	           "status": "downloaded", 
	           "results_extracted": { "$exists": True },
	           "ratings_calculated": { "$exists": False },
	           "$where": "this.results.length > 4"
	        }
	warm_cache(db, cache)
	env = trueskill.TrueSkill()
	for race in db.races.find(query).sort("race_date", 1).batch_size(500):
		rating_groups = []
		runner_groups = []
		non_finisher_runner_group = []
		non_finisher_ratings = []
		for runner in race["results"]:
			rating = get_rating(db, cache, runner["hid"])
			if is_finisher(runner):
				runner_groups.append((runner,))
				if rating == None:
					rating_groups.append((env.create_rating(),))
				else:
					rating_groups.append((env.create_rating(rating["mu"], rating["sigma"]),))
			else:
				non_finisher_runner_group.append(runner)
				if rating == None:
					non_finisher_ratings.append(env.create_rating())
				else:
					non_finisher_ratings.append(env.create_rating(rating["mu"], rating["sigma"]))
		if len(non_finisher_runner_group) > 0:
			runner_groups.append(tuple(non_finisher_runner_group))
			rating_groups.append(tuple(non_finisher_ratings))

		if len(runner_groups) >= 2:
			rated_rating_groups = env.rate(rating_groups)
			for runner_group, rating_group in zip(runner_groups, rated_rating_groups):
				for runner, rating in zip(runner_group, rating_group):
					save_rating(db, cache, {"hid": runner["hid"], "date": race["race_date"], "mu": rating.mu, "sigma": rating.sigma})
		race["ratings_calculated"] = True
		db.races.save(race)

def get_ma(period, mas):
	ma = {}
	mas_for_period = [ma for ma in mas if ma["period"] == period and ma["label"] == "normalized"]
	horse_mas = sorted(mas_for_period, key=lambda d: d["date"], reverse=True)
	if len(horse_mas) > 0:
		ma["value"] = round(horse_mas[0]["value"], 4)
		if len(horse_mas) >= 2:
			ma["grad"] = map(lambda m: round(m, 2), numpy.gradient(map(lambda d: d["value"], horse_mas[:8])))[:3]
		else:
			ma["grad"] = None
	else:
		ma["value"] = "None"
		ma["grad"] = "None"
	return ma

def days_between(date1, date2):
	if date1 == None:
		return None
	return (date2 - date1).days

def compute_weight(runner):
	weightDict = runner["weight"]
	lb = weightDict["lb"] or 0
	stone = weightDict["st"] or 0
	return int(stone) * 14 + int(lb)

def back_test():
	db = MongoClient().racing
	target_date = datetime(2014, 9, 1)
	for race in db.races.find({"race_date": target_date}):
		actual_speed = {}
		weights = {}
		ma3 = {}
		ma5 = {}
		ma8 = {}
		mus = {}
		sigmas = {}
		last_run = {}
		positions = []
		for runner in race["results"]:
			hid = runner["hid"]
			weights[hid] = compute_weight(runner)
			positions.append(hid)
			actual_speed[hid] = round(db.speeds.find_one({"hid": hid, "date": target_date})["normalized_speed"], 4)
			speeds = db.speeds.find({"hid": hid, "date": {"$lt": target_date}}).sort("date", 1)
			mas = calculate_moving_averages(speeds, hid)
			ma3[hid] = get_ma(3, mas)
			ma5[hid] = get_ma(5, mas)
			ma8[hid] = get_ma(8, mas)

			score = db.unbiased_ratings.find_one({"hid": hid, "date": {"$lt": target_date}}, sort=[("date", -1)])
			if score == None:
				mus[hid] = None
				sigmas[hid] = None
				last_run[hid] = None
			else:
				last_run[hid] = score["date"]
				mus[hid] = round(score["mu"], 4)
				sigmas[hid] = round(score["sigma"], 4)

		for hid in positions:
			sys.stdout.write("horse={0}\t".format(hid))
			sys.stdout.write("actual={0}\t".format(actual_speed[hid]))
			sys.stdout.write("mas3={0}\tma3grad={1}\t".format(ma3[hid]["value"], ma3[hid]["grad"]))
			sys.stdout.write("mas5={0}\tma5grad={1}\t".format(ma5[hid]["value"], ma5[hid]["grad"]))
			sys.stdout.write("mas8={0}\tma8grad={1}\t".format(ma8[hid]["value"], ma8[hid]["grad"]))
			sys.stdout.write("mu={0}\tsigma={1}\t".format(mus[hid], sigmas[hid]))
			sys.stdout.write("lastrun={0}\t".format(days_between(last_run[hid], target_date)))
			print "weight={0}".format(weights[hid])
		print "---------------"




