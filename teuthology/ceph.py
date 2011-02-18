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

def create_simple_monmap(test):
    """
    Writes a simple monmap based on current ceph.conf into <tmpdir>/monmap.

    Assumes test.ceph_conf is up to date.

    Assumes mon sections are named "mon.*", with the dot.
    """
    def gen_addresses():
        for section, data in test.ceph_conf.iteritems():
            PREFIX = 'mon.'
            if not section.startswith(PREFIX):
                continue
            name = section[len(PREFIX):]
            addr = data['mon addr']
            yield (name, addr)

    addresses = list(gen_addresses())
    assert addresses, "There are no monitors in config!"
    log.debug('Ceph mon addresses: %s', addresses)

    args = [
        '--create',
        '--clobber',
        ]
    for (name, addr) in addresses:
        args.extend(('--add', name, addr))
    args.extend([
            '--print',
            'monmap',
            ])
    utils.run(
        command=os.path.join(test.ceph_bindir, 'monmaptool'),
        args=args,
        stdout_tee=utils.TEE_TO_LOGS,
        stderr_tee=utils.TEE_TO_LOGS,
        )
