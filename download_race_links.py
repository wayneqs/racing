#!/usr/bin/env python

from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
import uuid

db = MongoClient().racing
root = "http://www.racingpost.com{0}"
for race in db.race_day.find({"status": "new"}).limit(4):
	#r = requests.get(root.format(race["race_url"]))
	#if r.status_code == 200:
	guid = uuid.uuid1()
	download_uri = "/Users/waynequinlivan/.racingcache/{0}".format(guid)
	#	soup = BeautifulSoup(r.text)
	#	fo = open("/Users/waynequinlivan/.racingcache/{0}".format(guid), "wb")
	#	fo.write(str(soup.select("div.popUp")[0]))
	#	fo.close()
	race["status"] = "downloaded"
	race["download_uri"] = download_uri
	print race