import os

from autotest_lib.client.bin import test
from autotest_lib.client.bin import utils

from teuthology import ceph

# TODO refactor to share code with cfuse_simple

class cfuse_pingpong(test.test):
    version = 1

    num_mon = 3
    num_osd = 1
    num_mds = 3

    def setup(self, **kwargs):
        ceph.get_binaries(self, kwargs.get('ceph_bin_url'))

    def run_once(self, **kwargs):
        print 'Entering tmp directory:', self.tmpdir
        os.chdir(self.tmpdir)

        # let ceph.conf use fixed pathnames
        os.symlink(self.resultsdir, 'results')
        os.mkdir('results/log')
        os.mkdir('results/profiling-logger')

        os.mkdir('dev')

        self.ceph_conf = ceph.skeleton_config(self)
        for id_ in range(self.num_mon):
            section = 'mon.{id}'.format(id=id_)
            self.ceph_conf.setdefault(section, {})
            self.ceph_conf[section]['mon addr'] = '{ip}:{port}'.format(
                ip=kwargs['server_ip'],
                port=6789+id_,
                )
        self.ceph_conf.write()

        self.ceph_bindir = ceph_bin = os.path.join(self.bindir, 'usr/local/bin')

        utils.system('{bindir}/osdmaptool --clobber --createsimple {num_osd} osdmap --pg_bits 2 --pgp_bits 4'.format(
                num_osd=self.num_osd,
                bindir=ceph_bin,
                conf=self.ceph_conf.filename,
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
                    conf=self.ceph_conf.filename,
                    ))
            proc = utils.BgJob(command='{bindir}/cmon -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            daemons.append(proc)

        os.unlink('monmap')
        os.unlink('osdmap')

        for id_ in range(self.num_osd):
            os.mkdir(os.path.join('dev', 'osd.{id}.data'.format(id=id_)))
            utils.system('{bindir}/cosd --mkfs -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=osd.{id} --cap mon "allow *" --cap osd "allow *" dev/osd.{id}.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    ))
            utils.system('{bindir}/ceph -c {conf} -k ceph.keyring -i dev/osd.{id}.keyring auth add osd.{id}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))

        for id_ in range(self.num_mds):
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=mds.{id} --cap mon "allow *" --cap osd "allow *" --cap mds "allow" dev/mds.{id}.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    ))
            utils.system('{bindir}/ceph -c {conf} -k ceph.keyring -i dev/mds.{id}.keyring auth add mds.{id}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))

        utils.system('{bindir}/ceph -c {conf} -k ceph.keyring mds set_max_mds {num_mds}'.format(
                bindir=ceph_bin,
                conf=self.ceph_conf.filename,
                num_mds=self.num_mds,
                ))

        for id_ in range(self.num_osd):
            proc = utils.BgJob(command='{bindir}/cosd -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            daemons.append(proc)

        for id_ in range(self.num_mds):
            proc = utils.BgJob(command='{bindir}/cmds -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            daemons.append(proc)

        ceph.wait_until_healthy(self)

        utils.system('{bindir}/ceph -s -c {conf}'.format(
                bindir=ceph_bin,
                conf=self.ceph_conf.filename,
                ))

        mnt1 = os.path.join(self.tmpdir, 'mnt1')
        os.mkdir(mnt1)
        fuse1 = utils.BgJob(command='{bindir}/cfuse -c {conf} {mnt}'.format(
                bindir=ceph_bin,
                conf=self.ceph_conf.filename,
                mnt=mnt1,
                ))

        mnt2 = os.path.join(self.tmpdir, 'mnt2')
        os.mkdir(mnt2)
        fuse2 = utils.BgJob(command='{bindir}/cfuse -c {conf} {mnt}'.format(
                bindir=ceph_bin,
                conf=self.ceph_conf.filename,
                mnt=mnt2,
                ))

        ceph.wait_until_fuse_mounted(self, fuse=fuse1, mountpoint=mnt1)
        ceph.wait_until_fuse_mounted(self, fuse=fuse2, mountpoint=mnt2)
        try:
            a1 = os.path.join(mnt1, 'a')
            with file(a1, 'w') as f:
                f.write('foo\n')

            a2 = os.path.join(mnt2, 'a')
            with file(a2) as f:
                data = f.read()
            assert data == 'foo\n'

            b2 = os.path.join(mnt2, 'b')
            os.rename(a2, b2)

            l = os.listdir(mnt1)
            assert l == ['b'], "Rename must be seen by fuse1, but it only saw: %r" % l
        finally:
            utils.system('fusermount -u {mnt}'.format(mnt=mnt1))
            utils.system('fusermount -u {mnt}'.format(mnt=mnt2))
            utils.join_bg_jobs([fuse1, fuse2])

            for d in daemons:
                d.sp.terminate()
            utils.join_bg_jobs(daemons)
