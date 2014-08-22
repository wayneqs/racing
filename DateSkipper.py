from datetime import datetime, timedelta

class DateSkipper:
	
	def __init__(self, start):
		self.current = start - timedelta(days=1)

	def __iter__(self):
		return self

	def next(self):
		if self.current.date() < datetime.now().date() - timedelta(days=1):
			self.current += timedelta(days=1)
			return self.current
		else:
			raise StopIteration
			