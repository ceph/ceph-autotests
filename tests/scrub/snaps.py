import os

from autotest_lib.client.bin import test
from autotest_lib.client.bin import utils

from teuthology import ceph

class snaps(test.test):
    version = 1

    num_mon = 3
    num_osd = 2
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

        self.ceph_conf = conf = os.path.join(self.bindir, 'ceph.conf')
        self.ceph_bindir = ceph_bin = os.path.join(self.bindir, 'usr/local/bin')

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

        ceph.wait_until_healthy(self)

        # build test
        utils.system('cp {cephconf} .'.format(cephconf=conf))
        utils.system(
            'g++ -I{bindir}/usr/local/include -I{bindir} -L{bindir}/usr/local/lib -lpthread -lrados -lcrush {bindir}/TestSnaps.cc -o testsnaps 100'.format(
                bindir=self.bindir))
        assert utils.system('LD_LIBRARY_PATH={bindir}/usr/local/lib ./testsnaps'.format(
                bindir=self.bindir)) == 0
