import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton

class ceph_mountloop(skeleton.CephTest):
    version = 1

    @skeleton.role('client')
    def do_150_ceph_mountloop(self):
        count = self.extra.get('count', 1000)
        count = int(count)
        for i in xrange(count):
            self.do_902_kernel_unmount()
            for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
                mnt = os.path.join(self.tmpdir, 'mnt.{id}'.format(id=id_))
                os.rmdir(mnt)
            self.do_072_kernel_mount()
