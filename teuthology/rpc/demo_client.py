from gevent import monkey; monkey.patch_all()

import logging

from teuthology.rpc import client

if __name__ == '__main__':
    import socket; socket.setdefaulttimeout(0.0001)
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        level=logging.DEBUG,
        )
    cl = client.Client(
        address=('0.0.0.0', 1234),
        cookie='xyzzy',
        )
    g1 = cl.call('sleepy', duration=1)
    g2 = cl.call('sleepy', duration=5, snore='bar')
    print g1.get()
    print g2.get()
    g3 = cl.call('quit')
    print g3.get()
