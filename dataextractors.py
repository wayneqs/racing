#!/usr/bin/python
# -*- coding: utf-8 -*-

import re

def extract_runners(soup):
	runners = []
	for tableBody in soup.select("table.resultRaceGrid tbody"):
		for race_items in tableBody.select("tr:nth-of-type(2)"):
			runner = {}
			hid = race_items["data-hid"].strip()
			position = race_items.select("td:nth-of-type(2) h3")[0].text.strip()
			dist = race_items.select("td:nth-of-type(3)")[0].text.strip()
			age = race_items.select("td:nth-of-type(5)")[0].text.strip()
			official_rating = race_items.select("td:nth-of-type(8)")[0].text.strip()
			weight, headgear = extract_impediments(race_items.select("td:nth-of-type(6)"))
			tid = extract_trainer(race_items.select("td:nth-of-type(7) a"))
			jid, claiming_weight = extract_jockey(tableBody.select("tr:nth-of-type(3)"))
			runner["hid"] = hid
			runner["position"] = position
			runner["dist"] = dist
			runner["age"] = age
			runner["official_rating"] = official_rating
			runner["weight"] = weight
			runner["weight"]["claiming_weight"] = claiming_weight
			runner["weight"]["total"] = compute_weight(runner) - claiming_weight
			runner["headgear"] = headgear
			runner["tid"] = tid
			runner["jid"] = jid
			runners.append(runner)
	return runners, None

def extract_trainer(soup):
	if len(soup) > 0:
		return re.search('\d+', soup[0].get("href").strip())
	return None

def extract_jockey(soup):
	if len(soup) > 0:
		jid = re.search('\d+', soup[0].select('a')[0].get("href").strip())
		tag = soup[0].select('sup')
		if len(tag) > 0:
			claiming_weight = tag[0].text.strip()
			claiming_weight = int(claiming_weight)
		return jid, claiming_weight
	return None, 0

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

def compute_weight(runner):
	weightDict = runner["weight"]
	lb = weightDict["lb"] or 0
	stone = weightDict["st"] or 0
	return int(stone) * 14 + int(lb)

def extract_impediments(soup):
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
	return (weight, headgear)

def extract_race_type(soup):
	try:
		h3 = soup.select("div.popUpHead h3")[0]
		m = re.findall(' Chase | Hurdle', h3.text)
		if len(m) == 0 or all(x == "" for x in m[0]):
			return "flat", None
		else:
			return "jumps", None
	except IndexError:
		return None, {"message": "Race title was not found"}

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

def extract_winning_time(soup):
	race_info = soup.select("div.raceInfo")
	if len(race_info) == 0:
		return None, {"message": "no_race_info"}
	else:
		if "TIME" not in race_info[0].text:
			return None, {"message": "no_time", "race_info": race_info[0].text}
		else:
			m = re.findall('TIME ([0-9]*\.?[0-9]+)s|TIME (\d+)m ([0-9]*\.?[0-9]+)s', race_info[0].text)
			if len(m) == 0 or all(x == "" for x in m[0]):
				return None, {"message": "time_format", "race_info": race_info[0].text}
			else:
				matches = m[0]
				try:
					if matches[0] != "":
						secs = float(matches[0])
						if secs >= 20:
							return secs, None
						elif secs < 20 and secs > 0:
							splits = matches[0].split(".")
							return float(splits[0]) * 60 + float(splits[1]), None
					else:
						return float(matches[1]) * 60 + float(matches[2]), None
				except ValueError:
					return None, {"message": "time_format", "race_info": race_info[0].text}
	return None, {"message": "mega failure"}

def get_aus_going(going_text):
	if re.search("fast|good 2", going_text, re.IGNORECASE) != None:
		return {"fast": 1, "medium": 0, "slow": 0}
	elif re.search("good 3|dead 4|dead 5", going_text, re.IGNORECASE) != None:
		return {"fast": 0, "medium": 1, "slow": 0}
	else:
		return {"fast": 0, "medium": 0, "slow": 1}

def get_usa_going(going_text):
	if re.search("firm|fast", going_text, re.IGNORECASE) != None:
		return {"fast": 1, "medium": 0, "slow": 0}
	elif re.search("good|cuppy|muddy", going_text, re.IGNORECASE) != None:
		return {"fast": 0, "medium": 1, "slow": 0}
	else:
		return {"fast": 0, "medium": 0, "slow": 1}

def get_going(going_text):
	if re.search("hard|firm|fast", going_text, re.IGNORECASE) != None:
		return {"fast": 1, "medium": 0, "slow": 0}
	elif re.search("good to firm|good|good to soft|yielding|standard to fast|standard|standard to slow", going_text, re.IGNORECASE) != None:
		return {"fast": 0, "medium": 1, "slow": 0}
	else:
		return {"fast": 0, "medium": 0, "slow": 1}

def get_flat_distance_category(distance):
	if distance <= 1540:
		return {"short": 1, "medium": 0, "long": 0}
	elif distance <= 2640:
		return {"short": 0, "medium": 1, "long": 0}
	else:
		return {"short": 0, "medium": 0, "long": 1}

def get_jumps_distance_category(distance):
	if distance <= 4400:
		return {"short": 1, "medium": 0, "long": 0}
	elif distance <= 6160:
		return {"short": 0, "medium": 1, "long": 0}
	else:
		return {"short": 0, "medium": 0, "long": 1}

def value_from_values(values, indices):
	for i in indices:
		if values[i] != "":
			return int(values[i])
	return 0

def compute_distance(matches):
	if len(matches) == 0 or all(x == "" for x in matches[0]):
		return None

	""" miles are in positions 0, 3, 5, 9 """
	miles = value_from_values(matches[0], [0, 3, 5, 9])
	""" furlongs are in positions 1, 6, 7, 10 """
	furlongs = value_from_values(matches[0], [1, 6, 7, 10])
	""" yards are in positions 2, 4, 8, 11 """
	yards = value_from_values(matches[0], [2, 4, 8, 11])

	return miles * 8 * 220 + furlongs * 220 + yards

def extract_race_going(soup):
	try:
		h1 = soup.select("h1")[0]
		countryMatches = re.findall(' (\(USA\)) | (\(AUS\)) ', h1.text)
		going_text = soup.select("div.popUpHead ul li")[0].text
		if len(countryMatches) == 0:
			return get_going(going_text), None
		else:
			if countryMatches[0] != "":
				return get_usa_going(going_text), None
			else:
				return get_aus_going(going_text), None
	except IndexError:
		return None, {"message": "Race title was not found"}

def extract_race_distance(soup):
	info = soup.select("div.popUpHead div.leftColBig ul li")
	if len(info) == 0:
		return None, {"message": "No header"}
	else:
		parentheful_matches = re.findall('\((\d+)m(\d+)f(\d+)y\)|\((\d+)m(\d+)y\)|\((\d+)m(\d+)f\)|\((\d+)f(\d+)y\)|\((\d+)m\)|\((\d+)f\)|\((\d+)y\)', info[0].text)
		parenthesisless_matches = re.findall('(\d+)m(\d+)f(\d+)y |(\d+)m(\d+)y |(\d+)m(\d+)f |(\d+)f(\d+)y |(\d+)m |(\d+)f |(\d+)y ', info[0].text)
		distance = compute_distance(parentheful_matches)
		if distance == None:
			distance = compute_distance(parenthesisless_matches)
			if distance == None or distance == 0:
				return None, {"message": "Distance format is bad"}
		race_type, errors = extract_race_type(soup)
		if errors == None:
			if race_type == "flat":
				return {"value": distance, "category": get_flat_distance_category(distance)}, None
			else:
				return {"value": distance, "category": get_jumps_distance_category(distance)}, None
		return None, errors
