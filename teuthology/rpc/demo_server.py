from gevent import monkey; monkey.patch_all()

import gevent.event
import gevent.server
import logging

from teuthology.rpc import server

log = logging.getLogger(__name__)

should_quit = gevent.event.AsyncResult()

def sleepy(duration, snore='zzz'):
    import time
    time.sleep(duration)
    return dict(snore=snore)

def quit():
    log.info('Quitting...')
    should_quit.set(True)
    return dict()

if __name__ == '__main__':
    import socket; socket.setdefaulttimeout(0.0001)
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        level=logging.DEBUG,
        )

    methods = dict(
        sleepy=sleepy,
        quit=quit,
        )
    handler = server.Handler(cookie='xyzzy', lookup=methods.get)
    srv = gevent.server.StreamServer(
        listener=('0.0.0.0', 1234),
        handle=handler,
        )
    srv.start()
    should_quit.wait()
    srv.stop()
