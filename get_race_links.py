#!/usr/bin/env python

from bs4 import BeautifulSoup
import requests
from datetime import datetime
from DateSkipper import DateSkipper
from RaceMetaDataExtractor import RaceMetaDataExtractor
from pymongo import MongoClient

def date_or_none(current_pointer):
	if current_pointer == None:
		return None
	else:
		return current_pointer["value"]

db = MongoClient().racing
epoch = datetime(2000, 1, 1)
current_pointer = db.current_pointer_collection.find_one({"type": "race_day"})
for d in DateSkipper(date_or_none(current_pointer) or epoch):
	r = requests.get("http://www.racingpost.com/horses2/results/home.sd?r_date={0}".format(d.strftime("%Y-%m-%d")))
	if r.status_code == 200:
		soup = BeautifulSoup(r.text)
		links = soup.select("ul.activeLink a")
		if links == None:
			db.race_day_errors.insert({"date": d, "url": r.url})
		else:
			for l in links:
				race_id, href = RaceMetaDataExtractor(l.get('href').replace('amp;','').replace('&popup=yes','')).extract()
				db.race_day.update({"race_id": race_id}, {"race_id": race_id, "race_url": href, "race_date": d, "status": "new"}, upsert=True)
			db.current_pointer_collection.update({"type": "race_day"}, {"type": "race_day", "value": d}, upsert=True)