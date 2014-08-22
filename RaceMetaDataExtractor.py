import re

class RaceMetaDataExtractor:
	
	def __init__(self, url):
		self.url = url

	def extract(self):
		m = re.search('race_id=(\d+)', self.url)
		if m == None:
			return None, None
		else:
			return m.group(1), self.url