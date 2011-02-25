import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton

class cfuse_fsx(skeleton.CephTest):
    version = 1

    @skeleton.role('client')
    def do_100_cfuse_mount(self):
        self.fuses = []
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            # TODO support kernel clients too; must use same role due
            # to "type" limitations
            mnt = os.path.join(self.tmpdir, 'mnt.{id}'.format(id=id_))
            os.mkdir(mnt)
            fuse = utils.BgJob(
                # we could use -m instead of ceph.conf, but as we need
                # ceph.conf to find the keyring anyway, it's not yet worth it

                command='{bindir}/cfuse -f -c {conf} --name=client.{id} {mnt}'.format(
                    bindir=self.ceph_bindir,
                    conf=self.ceph_conf.filename,
                    id=id_,
                    mnt=mnt,
                    ),
                stdout_tee=utils.TEE_TO_LOGS,
                stderr_tee=utils.TEE_TO_LOGS,
                )
            self.fuses.append((mnt, fuse))
            ceph.wait_until_fuse_mounted(self, fuse=fuse, mountpoint=mnt)

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

    @skeleton.role('client')
    def do_190_cfuse_unmount(self):
        for mnt, fuse in self.fuses:
            utils.system('fusermount -u {mnt}'.format(mnt=mnt))
            print 'Waiting for cfuse to exit...'
            utils.join_bg_jobs([fuse])
            assert fuse.result.exit_status == 0, \
                'cfuse failed with: %r' % fuse.result.exit_status
