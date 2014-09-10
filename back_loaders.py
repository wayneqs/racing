#!/usr/bin/env python
# -*- coding: utf-8 -*-

import horses
import dataextractors
from pymongo import MongoClient
from bs4 import BeautifulSoup
import sys
import numpy
import os
from bson.code import Code

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

def back_load_winning_time():
	back_load_race_property("winning_time_secs", dataextractors.extract_winning_time)

def back_load_runner_speeds():
	db = MongoClient().racing
	q = {"distance": {"$exists": True}, "winning_time_secs": {"$exists": True}}
	for race in db.races.find(q).batch_size(500):
		try:
			distance = race["distance"]["value"]
			time = race["winning_time_secs"]
			cumdist = 0
			raws = []
			normals = []
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

def capture_speeds(speeds, speedsn, runner):
	if speeds.get(runner["hid"]) == None:
		speeds[runner["hid"]] = []
	if speedsn.get(runner["hid"]) == None:
		speedsn[runner["hid"]] = []
	speeds[runner["hid"]].append(runner["speed"]["raw"])
	speedsn[runner["hid"]].append(runner["speed"]["normalized"])

def back_calculate_speed_emas():
	db = MongoClient().racing

def initialize(emas, hid):
	if emas.get(hid) == None: 
		emas[hid] = { "ema_tm1": -1, "eman_tm1": 0}

def seed_ma(period, db):
	mas = {}
	for race_type in ["flat", "jumps"]:
		for distance in ["long", "medium", "short"]:
			for going in ["fast", "medium", "slow"]:
				key = "{0}.{1}.{2}".format(race_type, distance, going)
				for horse in db.horses.find().batch_size(50):
					mas[horse["hid"]] = {key: {}}

	for race_type in ["flat", "jumps"]:
		for distance in ["long", "medium", "short"]:
			for going in ["fast", "medium", "slow"]:
				for hid in mas:
					key = "{0}.{1}.{2}".format(race_type, distance, going)
					hid = horse["hid"]
					distance_field = "distance.category.{0}".format(distance)
					going_field = "going.{0}".format(going)
					q = {going_field: 1, distance_field: 1, "race_type": race_type, "results.hid": hid, "results.speed.raw": {"$exists":True}}
					p = {"results.hid": 1, "results.speed.raw": 1, "results.speed.normalized": 1}
					raw_total = 0.0
					n_total = 0.0
					count = 0
					for race in db.races.find(q, p).sort("race_date", 1).limit(period):
						for runner in race["results"]:
							if runner["hid"] == hid and runner.get("speed") != None:
								count += 1
								raw_total += runner["speed"]["raw"]
								n_total += runner["speed"]["normalized"]
					if count > 0:
						mas[hid][key] = {"ema_tm1": raw_total / count, "eman_tm1": n_total / count}
	return mas

def back_calculate_mas_for_horses():
	db = MongoClient().racing
	db.mas.drop()
	emas3 = seed_ma(3, db)
	emas5 = seed_ma(5, db)
	for race_type in ["flat", "jumps"]:
		for distance in ["long", "medium", "short"]:
			for going in ["fast", "medium", "slow"]:
				key = "{0}.{1}.{2}".format(race_type, distance, going)
				try:
					distance_field = "distance.category.{0}".format(distance)
					going_field = "going.{0}".format(going)
					q = {going_field: 1, distance_field: 1, "race_type": race_type}
					races = db.races.find(q)
					for race in sorted(races, key=lambda d: d["race_date"]):
						dox = []
						date = race["race_date"]
						for runner in race["results"]:
							hid = runner["hid"]
							speed = runner.get("speed")
							

							if speed == None:
								continue

							value_t = speed["raw"]
							valuen_t = speed["normalized"]
							
							emas3_for_key = emas3[hid].get(key)
							ema3 = 0
							if emas3_for_key != None:
								ema3 = horses.compute_ema(3, value_t, emas3_for_key.get("ema_tm1"))
								emas3_for_key["ema_tm1"] = ema3
							
							eman3 = 0
							emans3_for_key = emas3[hid].get(key)
							if emans3_for_key != None:
								eman3 = horses.compute_ema(3, valuen_t, emans3_for_key.get("eman_tm1"))
								emans3_for_key["eman_tm1"] = ema3

							ema5 = 0
							emas5_for_key = emas3[hid].get(key)
							if emas5_for_key != None:
								ema5 = horses.compute_ema(5, value_t, emas5_for_key.get("ema_tm1"))
								emas5_for_key["ema_tm1"] = ema5
							
							eman5 = 0
							emans5_for_key = emas3[hid].get(key)
							if emans5_for_key != None:
								eman5 = horses.compute_ema(5, valuen_t, emans5_for_key.get("eman_tm1"))
								emans5_for_key["eman_tm1"] = eman5

							dox.append({"date": date, "hid": hid, "distance": distance, "going": going, "race_type": race_type,
						     "ema3": ema3, "value": value_t, "valuen": valuen_t, "ema5": ema5, "eman3": eman3, "eman5": eman5})
						if len(dox) > 0:
							db.mas.insert(dox)
				except:
					print "race_type={0}, distance={1}, going={2}".format(race_type, distance, going)
					raise	

def back_calculate_average_speeds_for_all():
	db = MongoClient().racing
	db.speeds_all.drop()

	for race_type in ["flat", "jumps"]:
		for distance in ["long", "medium", "short"]:
			for going in ["fast", "medium", "slow"]:
				all_speeds = []
				all_speedsn = []
				distance_field = "distance.category.{0}".format(distance)
				going_field = "going.{0}".format(going)
				q = {"winning_time_secs": {"$exists": True}, going_field: 1, distance_field: 1, "race_type": race_type}
				for race in db.races.find(q):
					for runner in race["results"]:
						all_speeds.append(runner["speed"]["raw"])
						all_speedsn.append(runner["speed"]["normalized"])
				r = numpy.array(all_speeds)
				rn = numpy.array(all_speedsn)
				d = {"distance": distance, "going": going, "race_type": race_type,
				     "max": numpy.amax(r), "min": numpy.amin(r),
				     "average": numpy.average(r), "mean": numpy.mean(r), "median": numpy.median(r), "std": numpy.std(r),
				     "average_n": numpy.average(rn), "mean_n": numpy.mean(rn), "median_n": numpy.median(rn), "std_n": numpy.std(rn)}
				db.speeds_all.insert(d)

def clean_dodgy_races():
	q = {"results.speed.raw" :{"$gt": 35}}
	db = MongoClient().racing
	for race in db.races.find(q):
		uri = race["download_uri"]
		os.remove(uri)
		db.races.remove({"_id": race["_id"]})


