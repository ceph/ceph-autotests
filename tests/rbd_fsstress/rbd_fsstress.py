import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton

class rbd_fsstress(skeleton.CephTest):
    version = 1

    @skeleton.role('client')
    def do_150_rbd_fsstress(self):
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            mnt = os.path.join(self.tmpdir, 'testimage{id}'.format(id=id_))

            self.job.run_test(
                'fsstress',
                testdir=mnt,
                extra_args=self.extra.get('rbd_fsstress_args', ''),
                )

            print 'rbd fsstress test ok'
