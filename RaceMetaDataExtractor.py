import re

class RaceMetaDataExtractor:
	
	def __init__(self, url):
		print url
		self.url = url

	def extract(self):
		m = re.search('race_id=(\d+)', self.url)
		if m == None:
			return None, None
		else:
			m.group(0), self.url