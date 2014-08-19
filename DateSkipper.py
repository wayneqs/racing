from datetime import datetime, timedelta

class DateSkipper:
	
	def __init__(self, start):
		self.current = start

	def __iter__(self):
		return self

	def next(self):
		if self.current.date() >= datetime.now().date():
			raise StopIteration
		else:
			self.current += timedelta(days=1)
			return self.current