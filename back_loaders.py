#!/usr/bin/env python

import horses
import dataextractors
from pymongo import MongoClient
from bs4 import BeautifulSoup
import sys
import numpy

def back_extract_distances():
	db = MongoClient().racing
	for race in db.races.find({"status": "downloaded"}).batch_size(500):
		uri = race["download_uri"]
		fo = open(uri, "r")
		soup = BeautifulSoup(fo.read())
		race = horses.extract_distance(db, race, soup)
		if race != None: 
			db.races.save(race)
		fo.close()

def back_calculate_horse_speed():
	db = MongoClient().racing
	for race in db.races.find({"status": "downloaded"}).batch_size(500):
		speed_stats, speeds = horses.calculate_horse_speeds(race)
		if speeds != None:
			db.speeds.insert(speeds)
		if speed_stats != None:
			speed_stats["race_id"] = race["race_id"]
			speed_stats["date"] = race["race_date"]
			db.speed_stats.insert(speed_stats)

def back_extract_race_times():
	db = MongoClient().racing
	for race in db.races.find({"status": "downloaded"}).batch_size(500):
		uri = race["download_uri"]
		fo = open(uri, "r")
		soup = BeautifulSoup(fo.read())
		race = horses.extract_race_time(db, race, soup)
		if race != None: 
			db.races.save(race)
		fo.close()

def rebuild_moving_averages():
	db = MongoClient().racing
	db.moving_averages.drop()
	dates = []
	normalized_speeds = []
	for horse in db.horses.find():
		mas = calculate_moving_averages(db.speeds.find({"hid": horse["hid"]}), horse["hid"])
		if len(mas) > 0:
			db.moving_averages.insert(mas)

def back_load_race_property(field, extractor):
	db = MongoClient().racing
	for race in db.races.find({"status": "downloaded"}).batch_size(500):
		uri = race["download_uri"]
		fo = open(uri, "r")
		soup = BeautifulSoup(fo.read())
		value, errors = extractor(soup)
		if errors != None:
			db.race_property_errors.save({"error": errors, "race": race, "property": field})
		else:
			race[field] = value
			db.races.save(race)
		fo.close()

def back_load_race_types():
	back_load_race_property("race_type", dataextractors.extract_race_type)

def back_load_race_going():
	back_load_race_property("going", dataextractors.extract_race_going)


def back_load_race_distance():
	back_load_race_property("distance", dataextractors.extract_race_distance)

def back_load_runner_speeds():
	db = MongoClient().racing
	for race in db.races.find({"status": "downloaded"}).batch_size(500):
		try:
			distance = race["distance"]["value"]
			time = race.get("winning_time_secs")
			if time == None:
				db.speed_errors.insert({"race": race, "status": "missing time"})
			else:
				cumdist = 0
				raws = []
				normals = []
				runner_count = len(race["results"])
				if runner_count == 0:
					db.speed_errors.insert({"race": race, "status": "no runners"})
				else:
					for runner in race["results"]:
						cumdist += dataextractors.extract_runner_distance(runner.get("dist"))
						runner_distance = distance - cumdist
						raw = runner_distance / time
						raws.append(raw)
						normalized = horses.normalize_speed(runner_distance, time)
						normals.append(normalized)
						runner["speed"] = {"raw": raw, "normalized": normalized}
					rawsarr = numpy.array(raws)
					normalssarr = numpy.array(normals)
					race["speed_stats"] = {"average": numpy.average(rawsarr), "mean": numpy.mean(rawsarr), "median": numpy.median(rawsarr), "std": numpy.std(rawsarr),
					                       "average_n": numpy.average(normalssarr), "mean_n": numpy.mean(normalssarr), "median_n": numpy.median(normalssarr), "std_n": numpy.std(normalssarr)}
					db.races.save(race)
		except KeyError:
			db.speed_errors.insert({"race": race, "status": "key error"})
