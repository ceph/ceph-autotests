import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton

from time import time,sleep

class scrub(skeleton.CephTest):
    version = 1

    @skeleton.role('osd.3')
    def do_100_thrash(self):
        finish = time() + 660
        while (time() < finish):
            assert len(self.daemons['osd']) == 1
            osd = self.daemons['osd'][0]
            self.daemons['osd'] = []
            osd.sp.terminate()
            utils.join_bg_jobs([osd])
            sleep(10)
            self.do_062_osd_start()
            sleep(110)

    @skeleton.role('client.0')
    def do_101_test_compile(self):
        # build test
        #utils.system('cp {cephconf} .'.format(cephconf=self.ceph_conf.filename))
        utils.system(
            'g++ -I{bindir}/usr/local/include -I{bindir} -L{bindir}/usr/local/lib -lpthread -lrados -lcrush {bindir}/TestSnaps.cc -o testsnaps'.format(
                bindir=self.bindir))

    @skeleton.role('client.0')
    def do_102_run_work(self):
        r = utils.system('LD_LIBRARY_PATH={bindir}/usr/local/lib ./testsnaps 660 500'.format(
                bindir=self.bindir))
        if r == 0:
            print "Run ok..."
        else:
            print r, "Errors"

    @skeleton.role('client.1')
    def do_103_run_scrub(self):
        finish = time() + 660
        while (time() < finish):
            utils.system('{bindir}/ceph pg scrub 2.0'.format(bindir=self.bindir))
            sleep(0.5)
