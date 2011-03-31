import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton

class rbd_librbd(skeleton.CephTest):
    version = 1

    @skeleton.role('client')
    def do_150_rbd_librbd(self):
        LD_LIB = os.getenv('LD_LIBRARY_PATH', '')
        os.putenv('LD_LIBRARY_PATH', LD_LIB + ':' + self.ceph_libdir)
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            utils.system('{bindir}/testlibrbdpp'.format(
                    bindir=self.ceph_bindir))
            print 'rbd testlibrbdpp ok'
            utils.system('{bindir}/testlibrbd'.format(
                    bindir=self.ceph_bindir))
            print 'rbd testlibrbd ok'
