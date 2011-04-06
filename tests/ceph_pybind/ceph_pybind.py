import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton

class ceph_pybind(skeleton.CephTest):
    version = 1

    @skeleton.role('client')
    def do_150_pybind_tests(self):
        os.environ["LD_LIBRARY_PATH"] = self.ceph_libdir
        os.environ["PYTHONPATH"] = self.ceph_pydir

        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            print "running ceph-pybind-test.py"
            utils.system('{bindir}/ceph-pybind-test.py'.format(
                bindir=self.ceph_bindir))
            print 'ceph python binding test ok'

