#!/usr/bin/env python

from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
import backoff
import uuid

root = "http://www.racingpost.com"

@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
@backoff.on_exception(backoff.expo, requests.exceptions.ConnectionError, max_tries=5)
def get_url(url):
	return requests.get(url)

db = MongoClient().racing
for race in db.race_day.find({"status": "new"}):
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
