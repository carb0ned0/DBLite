#!/usr/bin/env python

import heapq
import logging
import os
import pickle
import socket
import socketserver
import sys
import time
from collections import deque, namedtuple
from functools import wraps
from io import BytesIO
from optparse import OptionParser
from threading import get_ident as get_ident_t

try:
    import gevent
    from gevent.pool import Pool
    from gevent.server import StreamServer
    from gevent import get_ident, socket as gsock
    HAVE_GEVENT = True
except ImportError:
    HAVE_GEVENT = False
    gsock = socket
    Pool = StreamServer = None

__version__ = '0.1.0'
logger = logging.getLogger(__name__)

class Error(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

class ProtocolHandler:
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict,
            b'&': self.handle_set,
        }

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise EOFError()
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            rest = socket_file.readline().rstrip(b'\r\n')
            return first_byte + rest

    def handle_simple_string(self, sf):
        return sf.readline().rstrip(b'\r\n')

    def handle_error(self, sf):
        return Error(sf.readline().rstrip(b'\r\n'))

    def handle_integer(self, sf):
        number = sf.readline().rstrip(b'\r\n')
        if b'.' in number:
            return float(number)
        return int(number)

    def handle_string(self, sf):
        length = int(sf.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        return sf.read(length + 2)[:-2]

    def handle_array(self, sf):
        num = int(sf.readline().rstrip(b'\r\n'))
        return [self.handle_request(sf) for _ in range(num)]

    def handle_dict(self, sf):
        num = int(sf.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(sf) for _ in range(num * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def handle_set(self, sf):
        num = int(sf.readline().rstrip(b'\r\n'))
        return set([self.handle_request(sf) for _ in range(num)])

    def write_response(self, sf, data):
        buf = BytesIO()
        self._write(buf, data)
        sf.write(buf.getvalue())
        sf.flush()

    def _write(self, buf, data):
        if isinstance(data, bytes):
            buf.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, str):
            bdata = data.encode('utf-8')
            buf.write(b'$%d\r\n%s\r\n' % (len(bdata), bdata))
        elif isinstance(data, (int, float)):
            buf.write(b':%d\r\n' % data)
        elif isinstance(data, Error):
            buf.write(b'-%s\r\n' % str(data.message).encode('utf-8'))
        elif isinstance(data, (list, tuple, deque)):
            buf.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for k, v in data.items():
                self._write(buf, k)
                self._write(buf, v)
        elif isinstance(data, set):
            buf.write(b'&%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif data is None:
            buf.write(b'$-1\r\n')
        else:
            buf.write(b'+OK\r\n')

class ClientQuit(Exception): pass
class Shutdown(Exception): pass

Value = namedtuple('Value', ('data_type', 'value'))

KV = 0
HASH = 1
QUEUE = 2
SET = 3

class DBLiteServer:
    def __init__(self, host='127.0.0.1', port=31337, max_clients=1024, use_gevent=True):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.use_gevent = use_gevent and HAVE_GEVENT
        if self.use_gevent:
            self.pool = Pool(max_clients)
            self.server = StreamServer((host, port), self.handle_connection, spawn=self.pool)
        else:
            class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
                allow_reuse_address = True
            self.server = ThreadedTCPServer((host, port), self._threaded_handler_class())
        self.protocol = ProtocolHandler()
        self.commands = self.get_commands()
        self.kv = {}
        self.expiry = []
        self.expiry_map = {}
        self.stats = {'active_connections': 0, 'commands_processed': 0, 'command_errors': 0, 'connections': 0}

    def _threaded_handler_class(self):
        server = self
        class RequestHandler(socketserver.BaseRequestHandler):
            def handle(self):
                server.handle_connection(self.request, self.client_address)
        return RequestHandler

    def get_commands(self):
        return {
            b'SET': self.kv_set,
            b'GET': self.kv_get,
            b'DELETE': self.kv_delete,
            b'EXISTS': self.kv_exists,
            b'LPUSH': self.lpush,
            b'LPOP': self.lpop,
            b'HSET': self.hset,
            b'HGET': self.hget,
            b'SADD': self.sadd,
            b'SMEMBERS': self.smembers,
            b'EXPIRE': self.expire,
            b'FLUSHALL': self.flush_all,
            b'SAVE': self.save_to_disk,
            b'RESTORE': self.restore_from_disk,
            b'INFO': self.info,
            b'QUIT': self.client_quit,
            b'SHUTDOWN': self.shutdown,
        }

    def check_expired(self, key, ts=None):
        ts = ts or time.time()
        return key in self.expiry_map and ts > self.expiry_map[key]

    def clean_expired(self):
        ts = time.time()
        n = 0
        while self.expiry:
            eta, key = self.expiry[0]
            if eta > ts:
                break
            heapq.heappop(self.expiry)
            if self.expiry_map.get(key) == eta:
                del self.expiry_map[key]
                self.kv.pop(key, None)
                n += 1
        return n

    def unexpire(self, key):
        self.expiry_map.pop(key, None)

    def enforce_datatype(data_type, set_missing=True):
        def decorator(meth):
            @wraps(meth)
            def inner(self, key, *args, **kwargs):
                self.clean_expired()
                if self.check_expired(key):
                    self.kv.pop(key, None)
                if key in self.kv:
                    value = self.kv[key]
                    if value.data_type != data_type:
                        raise Error('WRONGTYPE Operation against a key holding the wrong kind of value')
                elif set_missing:
                    if data_type == KV:
                        val = ''
                    elif data_type == HASH:
                        val = {}
                    elif data_type == QUEUE:
                        val = deque()
                    elif data_type == SET:
                        val = set()
                    self.kv[key] = Value(data_type, val)
                return meth(self, key, *args, **kwargs)
            return inner
        return decorator

    def kv_set(self, key, value):
        self.unexpire(key)
        self.kv[key] = Value(KV, value)
        return 'OK'

    def kv_get(self, key):
        self.clean_expired()
        if self.check_expired(key):
            self.kv.pop(key, None)
            return None
        if key in self.kv:
            return self.kv[key].value
        return None

    def kv_delete(self, key):
        self.clean_expired()
        if key in self.kv:
            del self.kv[key]
            self.unexpire(key)
            return 1
        return 0

    def kv_exists(self, key):
        self.clean_expired()
        if self.check_expired(key):
            self.kv.pop(key, None)
            return 0
        return 1 if key in self.kv else 0

    @enforce_datatype(QUEUE)
    def lpush(self, key, *values):
        self.kv[key].value.extendleft(values)
        return len(self.kv[key].value)

    @enforce_datatype(QUEUE)
    def lpop(self, key):
        try:
            return self.kv[key].value.popleft()
        except IndexError:
            return None

    @enforce_datatype(HASH)
    def hset(self, key, field, value):
        self.kv[key].value[field] = value
        return 1

    @enforce_datatype(HASH)
    def hget(self, key, field):
        return self.kv[key].value.get(field)

    @enforce_datatype(SET)
    def sadd(self, key, *members):
        added = 0
        for member in members:
            if member not in self.kv[key].value:
                self.kv[key].value.add(member)
                added += 1
        return added

    @enforce_datatype(SET)
    def smembers(self, key):
        return list(self.kv[key].value)

    def expire(self, key, seconds):
        if key not in self.kv:
            return 0
        eta = time.time() + int(seconds)
        self.expiry_map[key] = eta
        heapq.heappush(self.expiry, (eta, key))
        return 1

    def flush_all(self):
        self.kv.clear()
        self.expiry = []
        self.expiry_map = {}
        return 'OK'

    def save_to_disk(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump({'kv': self.kv, 'expiry_map': self.expiry_map}, f)
        return 'OK'

    def restore_from_disk(self, filename):
        if not os.path.exists(filename):
            return 0
        with open(filename, 'rb') as f:
            state = pickle.load(f)
        self.kv = state['kv']
        self.expiry_map = state['expiry_map']
        self.expiry = []
        for key, eta in self.expiry_map.items():
            heapq.heappush(self.expiry, (eta, key))
        return 1

    def info(self):
        self.clean_expired()
        return {
            'active_connections': self.stats['active_connections'],
            'commands_processed': self.stats['commands_processed'],
            'command_errors': self.stats['command_errors'],
            'connections': self.stats['connections'],
            'keys': len(self.kv),
        }

    def client_quit(self):
        raise ClientQuit()

    def shutdown(self):
        raise Shutdown()

    def run(self):
        logger.info(f"Starting DBLite server on {self.host}:{self.port}")
        self.server.serve_forever()

    def handle_connection(self, conn, addr):
        self.stats['active_connections'] += 1
        self.stats['connections'] += 1
        logger.debug(f"Connection from {addr}")
        sf = conn.makefile('rwb')
        while True:
            try:
                data = self.protocol.handle_request(sf)
                if not isinstance(data, list):
                    data = data.split()
                resp = self.respond(data)
            except EOFError:
                break
            except ClientQuit:
                break
            except Shutdown:
                self.server.shutdown()
                break
            except Error as e:
                resp = e
                self.stats['command_errors'] += 1
            except Exception as e:
                resp = Error(str(e))
                self.stats['command_errors'] += 1
            self.protocol.write_response(sf, resp)
            self.stats['commands_processed'] += 1
        sf.close()
        conn.close()
        self.stats['active_connections'] -= 1

    def respond(self, data):
        if not data:
            raise Error('Empty request')
        cmd = data[0].upper() if isinstance(data[0], (bytes, str)) else data[0]
        if cmd not in self.commands:
            raise Error(f"Unknown command: {cmd}")
        return self.commands[cmd](*data[1:])

class Client:
    def __init__(self, host='127.0.0.1', port=31337):
        self.host = host
        self.port = port
        self.protocol = ProtocolHandler()

    def decode_resp(self, resp):
        if isinstance(resp, bytes):
            try:
                return resp.decode('utf-8')
            except UnicodeDecodeError:
                return resp
        elif isinstance(resp, list):
            return [self.decode_resp(i) for i in resp]
        elif isinstance(resp, set):
            return {self.decode_resp(i) for i in resp}
        elif isinstance(resp, dict):
            return {self.decode_resp(k): self.decode_resp(v) for k, v in resp.items()}
        else:
            return resp

    def execute(self, cmd, *args):
        with gsock.socket() as s:
            s.connect((self.host, self.port))
            sf = s.makefile('rwb')
            self.protocol.write_response(sf, [cmd] + list(args))
            resp = self.protocol.handle_request(sf)
            if isinstance(resp, Error):
                raise Exception(resp.message)
            return self.decode_resp(resp)

    def set(self, key, value):
        return self.execute('SET', key, value)

    def get(self, key):
        return self.execute('GET', key)

    def delete(self, key):
        return self.execute('DELETE', key)

    def exists(self, key):
        return self.execute('EXISTS', key)

    def lpush(self, key, *values):
        return self.execute('LPUSH', key, *values)

    def lpop(self, key):
        return self.execute('LPOP', key)

    def hset(self, key, field, value):
        return self.execute('HSET', key, field, value)

    def hget(self, key, field):
        return self.execute('HGET', key, field)

    def sadd(self, key, *members):
        return self.execute('SADD', key, *members)

    def smembers(self, key):
        return self.execute('SMEMBERS', key)

    def expire(self, key, seconds):
        return self.execute('EXPIRE', key, seconds)

    def flushall(self):
        return self.execute('FLUSHALL')

    def save(self, filename):
        return self.execute('SAVE', filename)

    def restore(self, filename):
        return self.execute('RESTORE', filename)

    def info(self):
        return self.execute('INFO')

    def quit(self):
        return self.execute('QUIT')

    def shutdown(self):
        return self.execute('SHUTDOWN')

def main():
    parser = OptionParser()
    parser.add_option('-H', '--host', default='127.0.0.1')
    parser.add_option('-p', '--port', default=31337, type=int)
    parser.add_option('-d', '--debug', action='store_true')
    options, args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if options.debug else logging.INFO)
    server = DBLiteServer(host=options.host, port=options.port)
    try:
        server.run()
    except KeyboardInterrupt:
        logger.info("Shutting down")

if __name__ == '__main__':
    main()