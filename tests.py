import unittest
from primes import is_prime

class DateSkipperTests(unittest.TestCase):
    """Tests for `DateSkipper.py`."""

    def should_iterate_over_dates(self):
        """Iterates a bunch of dates"""
        self.assertTrue(is_prime(5))

if __name__ == '__main__':
    unittest.main()