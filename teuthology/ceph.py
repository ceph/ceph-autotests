import logging

from autotest_lib.client.bin import utils

log = logging.getLogger(__name__)

def wait_until_healthy(job):
    """Wait until a Ceph cluster is healthy."""
    while True:
        health = utils.run('{bindir}/ceph -c {conf} health --concise'.format(
                bindir=job.ceph_bindir,
                conf=job.ceph_conf,
                ))
        log.debug('Ceph health: %s', health)
        if health.stdout.split(None, 1)[0] == 'HEALTH_OK':
            break
