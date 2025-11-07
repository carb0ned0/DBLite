import unittest
import time
import socket
import subprocess

class TestDBLite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.log_file = open('debug.log', 'w')
        cls.server_process = subprocess.Popen(['python', 'dblite.py', '-d'], stdout=cls.log_file, stderr=subprocess.STDOUT)
        if not cls.wait_for_server('127.0.0.1', 31337):
            raise RuntimeError("Server did not start within 5 seconds")
        from dblite import Client
        cls.c = Client()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.c.shutdown()
        except:
            pass
        cls.server_process.wait(timeout=5)
        cls.log_file.close()

    @staticmethod
    def wait_for_server(host, port, timeout=10):
        start_time = time.time()
        sleep_time = 0.1
        while time.time() - start_time < timeout:
            try:
                with socket.socket() as s:
                    s.connect((host, port))
                return True
            except ConnectionRefusedError:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 1.5, 1)
        return False

    def setUp(self):
        self.c.flushall()

    def test_set_get(self):
        self.c.set('key1', 'value1')
        self.assertEqual(self.c.get('key1'), 'value1')
        self.c.set('key2', 123)
        self.assertEqual(self.c.get('key2'), 123)
        self.c.set('key3', b'binary')
        self.assertEqual(self.c.get('key3'), 'binary')
        self.c.set('key4', b'\x80')
        self.assertEqual(self.c.get('key4'), b'\x80')
        self.assertIsNone(self.c.get('nonexistent'))

    def test_delete(self):
        self.c.set('key1', 'value1')
        self.assertEqual(self.c.delete('key1'), 1)
        self.assertIsNone(self.c.get('key1'))
        self.assertEqual(self.c.delete('nonexistent'), 0)

    def test_exists(self):
        self.c.set('key1', 'value1')
        self.assertEqual(self.c.exists('key1'), 1)
        self.assertEqual(self.c.exists('nonexistent'), 0)

    def test_lpush_lpop(self):
        self.c.lpush('mylist', 'a', 'b', 'c')
        self.assertEqual(self.c.lpop('mylist'), 'c')
        self.assertEqual(self.c.lpop('mylist'), 'b')
        self.assertEqual(self.c.lpop('mylist'), 'a')
        self.assertIsNone(self.c.lpop('mylist'))

    def test_hset_hget(self):
        self.c.hset('myhash', 'field1', 'value1')
        self.c.hset('myhash', 'field2', 'value2')
        self.assertEqual(self.c.hget('myhash', 'field1'), 'value1')
        self.assertEqual(self.c.hget('myhash', 'field2'), 'value2')
        self.assertIsNone(self.c.hget('myhash', 'nonexistent'))

    def test_sadd_smembers(self):
        self.c.sadd('myset', 'one', 'two', 'three')
        members = self.c.smembers('myset')
        self.assertEqual(set(members), {'one', 'two', 'three'})

    def test_expire(self):
        self.c.set('key1', 'value1')
        self.c.expire('key1', 1)
        time.sleep(0.5)
        self.assertEqual(self.c.get('key1'), 'value1')
        time.sleep(2)
        self.assertIsNone(self.c.get('key1'))

    def test_save_restore(self):
        self.c.set('key1', 'value1')
        self.c.set('key_bin', b'\x80')
        self.c.lpush('mylist', 'a', 'b')
        self.c.save('test.dump')
        self.c.flushall()
        self.assertIsNone(self.c.get('key1'))
        self.c.restore('test.dump')
        self.assertEqual(self.c.get('key1'), 'value1')
        self.assertEqual(self.c.get('key_bin'), b'\x80')
        self.assertEqual(self.c.lpop('mylist'), 'b')

    def test_type_enforcement(self):
        self.c.set('key1', 'value1')
        with self.assertRaises(Exception):
            self.c.lpush('key1', 'a')
        self.c.lpush('key2', 'a')
        with self.assertRaises(Exception):
            self.c.hset('key2', 'field', 'value')

    def test_info(self):
        info = self.c.info()
        self.assertEqual(info['keys'], 0)
        self.c.set('key1', 'value1')
        info = self.c.info()
        self.assertEqual(info['keys'], 1)

if __name__ == '__main__':
    unittest.main()