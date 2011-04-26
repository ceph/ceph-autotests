import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton

class rbd_ffsb(skeleton.CephTest):
    version = 1

    @skeleton.role('client')
    def do_150_rbd_ffsb(self):
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            mnt = os.path.join(self.tmpdir, 'testimage{id}'.format(id=id_))

            self.job.run_test(
                'ffsb',
                dir=mnt,
                profiles=self.extra.get('rbd_ffsb_profiles'),
                tag=self.generate_tag_for_subjob(client_id=id_),
                )

            print 'rbd ffsb test ok'
