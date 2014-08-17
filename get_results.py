#!/usr/bin/env python

import datetime
from bs4 import BeautifulSoup
import requests
from datetime import date
from DateSkipper import DateSkipper

epoch = datetime.date(2014, 8, 10)
for d in DateSkipper(epoch):
	#r = requests.get('http://www.racingpost.com/horses2/results/home.sd?r_date=2014-08-15')
	print d