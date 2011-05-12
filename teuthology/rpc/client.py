import errno
import gevent
import itertools
import json
import logging
import socket
import time

from . import util

log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    )

def connect_forever(sock, address):
    MAX_DELAY = 30
    delay = 0.1

    while True:
        try:
            sock.connect(address)
        except socket.error as e:
            if e.errno in [
                errno.ECONNREFUSED,
                errno.ECONNABORTED,
                ]:
                pass
            else:
                raise
        else:
            break
        log.info('Waiting for server, delaying %ds', delay)
        time.sleep(delay)
        delay *= 2
        if delay > MAX_DELAY:
            delay = MAX_DELAY

class RPCError(Exception):
    """RPC Error"""

    def __init__(self, msg, code):
        self.msg = msg
        self.code = code

    def __str__(self):
        message = self.msg
        if self.code is not None:
            message = '{code}: {message}'.format(code=self.code, message=message)
        return '{name}: {message}'.format(name=self.__doc__, message=message)

class Client(object):
    def __init__(self, address, cookie):
        self.cookie = cookie
        self.socket = socket.socket()
        self.socket.settimeout(None)
        connect_forever(self.socket, address)
        self.id = itertools.count()
        self.pending = {}
        self.receiver = gevent.spawn(self._receive)

    def _receive(self):
        self.socket.sendall(self.cookie + '\n')
        for line in util.readlines(self.socket):
            log.debug('Recv: %r', line)
            data = json.loads(line)
            id_ = data['id']
            res = self.pending[id_]
            if data.get('status', None) != 'ok':
                msg = data.get('msg', 'Unknown RPC error: {data}'.format(data=data))
                code = data.get('code')
                res.set_exception(RPCError(msg=msg, code=code))
            else:
                res.set(data.get('data'))

        # EOF
        if self.pending:
            log.warning('Connection closed early on %d requests', len(self.pending))
            for k,v in self.pending.items():
                v.set_exception(socket.error(errno.EBADF, 'Connection closed already'))

    def _call(self, method, **kw):
        id_ = next(self.id)
        assert id_ not in self.pending
        res = gevent.event.AsyncResult()
        msg = dict(
            id=id_,
            method=method,
            args=kw,
            )
        data = json.dumps(msg) + '\n'
        log.debug('Send: %r', data)
        self.socket.sendall(data)
        self.pending[id_] = res
        return res.get()

    def call(self, method, *a, **kw):
        g = gevent.spawn(self._call, method, *a, **kw)
        return g
