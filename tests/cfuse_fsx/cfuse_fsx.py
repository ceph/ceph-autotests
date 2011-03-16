import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton

class cfuse_fsx(skeleton.CephTest):
    version = 1


    @skeleton.role('client')
    def do_150_cfuse_fsx(self):
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            # TODO parallel?
            mnt = os.path.join(self.tmpdir, 'mnt.{id}'.format(id=id_))

            self.job.run_test(
                'fsx',
                dir=mnt,
                )

            print 'cfuse fsx test ok'
