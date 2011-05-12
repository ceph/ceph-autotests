import errno
import gevent
import json
import logging
import socket
import types

from . import util

log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    )

class RPCError(Exception):
    pass

def serve(req, sock, lookup):
    log.debug('Starting to serve %r', req)
    try:
        try:
            id_ = req['id']
        except KeyError:
            raise RPCError('Request missing "id"')
        try:
            method = req['method']
        except KeyError:
            raise RPCError('Request missing "method"')
        fn = lookup(method)
        if fn is None:
            raise RPCError('Unknown method.')
        res = fn(**req.get('args', []))
    except Exception as e:
        log.exception('RPC call failed: method %r', method)
        # TODO pass structured data from some exceptions to caller?
        msg = dict(
            id=id_,
            status='error',
            code=e.__class__.__name__,
            msg=str(e),
            )
    else:
        msg = dict(
            id=id_,
            status='ok',
            data=res,
            )
    msg = json.dumps(msg) + "\n"
    try:
        # TODO this might not be an atomic send? if json sent to
        # client is ever corrupted, wrap this with a lock
        sock.sendall(msg)
    except socket.error as e:
        if e.errno == errno.EBADF:
            # connection was closed while we were processing, ignore silently
            log.debug('Silencing response to a closed socket, id %r', id_)
            pass
        else:
            raise
    log.debug('Done serving %r', req)

class Handler(object):
    def __init__(self, cookie, lookup):
        self.cookie = cookie
        self.lookup = lookup

    def __call__(self, sock, address):
        log.info('New connection from %s:%d', *address)
        sock.settimeout(None)
        try:
            lines = util.readlines(sock)
            hello = next(lines)
            if hello != self.cookie:
                log.warning('Bad hello from %r: %r', address, hello)
                sock.sendall('You forgot to say please!\n')
                return
            for line in lines:
                log.debug('Got line %r', line)
                try:
                    req = json.loads(line)
                except ValueError:
                    log.warning('Cannot decode JSON: %r', line)
                    break

                log.info('Serving request %r', req)
                g = gevent.Greenlet(
                    serve,
                    req=req,
                    sock=sock,
                    lookup=self.lookup,
                    )
                g.start()
        finally:
            sock.close()
            log.info('Closed connection %s:%d', *address)
