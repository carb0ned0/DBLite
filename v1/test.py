import unittest
from dblite import Client

class TestDBLite(unittest.TestCase):
    def setUp(self):
        self.c = Client()
        self.c.flushall()

    def test_kv(self):
        self.c.set('k1', 'v1')
        self.assertEqual(self.c.get('k1'), 'v1')
        self.c.expire('k1', 1)
        time.sleep(2)
        self.assertIsNone(self.c.get('k1'))

    # Add tests for lists, hashes, sets, persistence, etc.

if __name__ == '__main__':
    unittest.main()