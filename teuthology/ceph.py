import logging
import os
import time

from autotest_lib.client.bin import utils

log = logging.getLogger(__name__)

def get_binaries(job, url=None):
    """Fetch and unpack Ceph binary tarball."""
    # TODO autodetect architecture
    CEPH_BIN_DEFAULT_URL = 'http://ceph.newdream.net/gitbuilder/tarball/ref/origin_master.tgz'
    if url is None:
        url = CEPH_BIN_DEFAULT_URL
    tarball = os.path.join(job.tmpdir, 'ceph-bin.tgz')
    utils.get_file(url, tarball)
    utils.system('tar xzf {tarball} -C {bindir}'.format(tarball=tarball, bindir=job.bindir))
    log.info('Finished unpacking binary in: %s', job.bindir)

def wait_until_healthy(job):
    """Wait until a Ceph cluster is healthy."""
    while True:
        health = utils.run(
            '{bindir}/ceph -c {conf} health --concise'.format(
                bindir=job.ceph_bindir,
                conf=job.ceph_conf,
                ),
            verbose=False,
            )
        log.debug('Ceph health: %s', health.stdout.rstrip('\n'))
        if health.stdout.split(None, 1)[0] == 'HEALTH_OK':
            break
        time.sleep(1)

def wait_until_fuse_mounted(job, fuse, mountpoint):
    while True:
        result = utils.run(
            "stat --file-system --printf='%T\n' -- {mnt}".format(mnt=mountpoint),
            verbose=False,
            )
        fstype = result.stdout.rstrip('\n')
        if fstype == 'fuseblk':
            break
        log.debug('cfuse not yet mounted, got fs type {fstype!r}'.format(fstype=fstype))

        # it shouldn't have exited yet; exposes some trivial problems
        assert fuse.sp.poll() is None

        time.sleep(5)
    log.info('cfuse is mounted on %s', mountpoint)
