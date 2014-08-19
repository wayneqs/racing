#!/usr/bin/env python

import datetime
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from DateSkipper import DateSkipper
from RaceMetaDataExtractor import RaceMetaDataExtractor
from pymongo import MongoClient

def find_current_pointer(db):
	result = db.current_pointer_collection.find_one({"type": "race_day"})
	if result == None:
		return None
	else:
		return result["value"]

db = MongoClient().racing
epoch = datetime(2014, 8, 18)
start = find_current_pointer(db)
for d in DateSkipper(start or epoch):
	r = requests.get("http://www.racingpost.com/horses2/results/home.sd?r_date={0}".format(d.strftime("%Y-%m-%d")))
	if r.status_code == 200:
		soup = BeautifulSoup(r.text)
		links = soup.select("ul.activeLink a")
		if links == None:
			print r
		else:
			for l in links:
				race_id, href = RaceMetaDataExtractor(l.get('href').replace('amp;','').replace('&popup=yes','')).extract()
				db.race_day.update({"race_id": race_id}, {"race_id": race_id, "race_url": href}, upsert=True)
			db.current_pointer_collection.update({"type": "race_day"}, {"type": "race_day", "value": d}, upsert=True)