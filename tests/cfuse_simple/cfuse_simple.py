import os
import time

from autotest_lib.client.bin import test
from autotest_lib.client.bin import utils

class cfuse_simple(test.test):
    version = 1

    num_mon = 3
    num_osd = 1
    num_mds = 3

    def setup(self):
        url = 'http://ceph.newdream.net/gitbuilder/tarball/ref/origin_master.tgz'
        tarball = os.path.join(self.tmpdir, 'ceph-bin.tgz')
        utils.get_file(url, tarball)
        utils.system('tar xzf {tarball} -C {bindir}'.format(tarball=tarball, bindir=self.bindir))
        print 'Finished unpacking binary in:', self.bindir

    def run_once(self):
        print 'Entering tmp directory:', self.tmpdir
        os.chdir(self.tmpdir)

        # let ceph.conf use fixed pathnames
        os.symlink(self.resultsdir, 'results')
        os.mkdir('results/log')
        os.mkdir('results/profiling-logger')

        os.mkdir('dev')

        conf = os.path.join(self.bindir, 'ceph.conf')
        ceph_bin = os.path.join(self.bindir, 'usr/local/bin')

        utils.system('{bindir}/osdmaptool --clobber --createsimple {num_osd} osdmap --pg_bits 2 --pgp_bits 4'.format(
                num_osd=self.num_osd,
                bindir=ceph_bin,
                conf=conf,
                ))

        utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=mon. ceph.keyring'.format(
                bindir=ceph_bin,
                ))

        utils.system('{bindir}/cauthtool --gen-key --name=client.admin --set-uid=0 --cap mon "allow *" --cap osd "allow *" --cap mds "allow" ceph.keyring'.format(
                bindir=ceph_bin,
                ))

        utils.system('{bindir}/monmaptool --create --clobber --add 0 127.0.0.1:6789 --add 1 127.0.0.1:6790 --add 2 127.0.0.1:6791 --print monmap'.format(
                bindir=ceph_bin,
                ))

        daemons = []

        for id_ in range(self.num_mon):
            utils.system('{bindir}/cmon --mkfs -i {id} -c {conf} --monmap=monmap --osdmap=osdmap --keyring=ceph.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=conf,
                    ))
            proc = utils.BgJob(command='{bindir}/cmon -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=conf,
                    ))
            daemons.append(proc)

        os.unlink('monmap')
        os.unlink('osdmap')

        for id_ in range(self.num_osd):
            os.mkdir(os.path.join('dev', 'osd.{id}.data'.format(id=id_)))
            utils.system('{bindir}/cosd --mkfs -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=conf,
                    ))
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=osd.{id} --cap mon "allow *" --cap osd "allow *" dev/osd.{id}.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    ))
            utils.system('{bindir}/ceph -c {conf} -k ceph.keyring -i dev/osd.{id}.keyring auth add osd.{id}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=conf,
                    ))

        for id_ in range(self.num_mds):
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=mds.{id} --cap mon "allow *" --cap osd "allow *" --cap mds "allow" dev/mds.{id}.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    ))
            utils.system('{bindir}/ceph -c {conf} -k ceph.keyring -i dev/mds.{id}.keyring auth add mds.{id}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=conf,
                    ))

        utils.system('{bindir}/ceph -c {conf} -k ceph.keyring mds set_max_mds {num_mds}'.format(
                bindir=ceph_bin,
                conf=conf,
                num_mds=self.num_mds,
                ))

        for id_ in range(self.num_osd):
            proc = utils.BgJob(command='{bindir}/cosd -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=conf,
                    ))
            daemons.append(proc)

        for id_ in range(self.num_mds):
            proc = utils.BgJob(command='{bindir}/cmds -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=conf,
                    ))
            daemons.append(proc)

        # wait until ceph is healthy
        while True:
            health = utils.run('{bindir}/ceph -c {conf} health --concise'.format(
                    bindir=ceph_bin,
                    conf=conf,
                    ))
            print 'Ceph health:', health
            if health.stdout.split(None, 1)[0] == 'HEALTH_OK':
                break

        utils.system('{bindir}/ceph -s -c {conf}'.format(
                bindir=ceph_bin,
                conf=conf,
                ))

        mnt = os.path.join(self.tmpdir, 'mnt')
        os.mkdir(mnt)
        fuse = utils.BgJob(command='{bindir}/cfuse -c {conf} {mnt}'.format(
                bindir=ceph_bin,
                conf=conf,
                mnt=mnt,
                ))

        while True:
            result = utils.run("stat --file-system --printf='%T\n' -- {mnt}".format(mnt=mnt))
            fstype = result.stdout.rstrip('\n')
            if fstype == 'fuseblk':
                break
            print 'cfuse not yet mounted, got fs type {fstype!r}'.format(fstype=fstype)

            # it shouldn't have exited yet; exposes some trivial problems
            assert fuse.sp.poll() is None

            time.sleep(1)
        print 'Confirmed: cfuse is mounted.'

        try:
            one = os.path.join(mnt, 'one')
            with file(one, 'w') as f:
                f.write('foo\n')
            two = os.path.join(mnt, 'two')
            os.rename(one, two)
            with file(two) as f:
                data = f.read()
            assert data == 'foo\n'
        finally:
            utils.system('fusermount -u {mnt}'.format(mnt=mnt))
            utils.join_bg_jobs([fuse])

            for d in daemons:
                d.sp.terminate()
            utils.join_bg_jobs(daemons)
