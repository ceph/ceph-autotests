import configobj
import logging
import os
import time

from autotest_lib.client.bin import utils

log = logging.getLogger(__name__)

def get_binaries(test, url=None):
    """Fetch and unpack Ceph binary tarball."""
    # TODO autodetect architecture
    CEPH_BIN_DEFAULT_URL = 'http://ceph.newdream.net/gitbuilder/tarball/ref/origin_master.tgz'
    if url is None:
        url = CEPH_BIN_DEFAULT_URL
    tarball = os.path.join(test.tmpdir, 'ceph-bin.tgz')
    utils.get_file(url, tarball)
    utils.system('tar xzf {tarball} -C {bindir}'.format(tarball=tarball, bindir=test.bindir))
    log.info('Finished unpacking binary in: %s', test.bindir)

def wait_until_healthy(test):
    """Wait until a Ceph cluster is healthy."""
    while True:
        health = utils.run(
            '{bindir}/ceph -c {conf} health --concise'.format(
                bindir=test.ceph_bindir,
                conf=test.ceph_conf.filename,
                ),
            verbose=False,
            )
        log.debug('Ceph health: %s', health.stdout.rstrip('\n'))
        if health.stdout.split(None, 1)[0] == 'HEALTH_OK':
            break
        time.sleep(1)

def wait_until_fuse_mounted(test, fuse, mountpoint):
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

def skeleton_config(job):
    """
    Returns a ConfigObj that's prefilled with a skeleton config.

    Use conf[section][key]=value or conf.merge to change it.

    Use conf.write to write it out, override .filename first if you want.
    """
    path = os.path.join(os.path.dirname(__file__), 'ceph.conf')
    o = configobj.ConfigObj(path, file_error=True)
    # override this if you don't like it
    o.filename = os.path.join(job.tmpdir, 'ceph.conf')
    return o
