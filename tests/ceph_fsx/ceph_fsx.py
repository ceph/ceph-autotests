import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton

class ceph_fsx(skeleton.CephTest):
    version = 1

    @skeleton.role('client')
    def do_100_ceph_mount(self):
        self.mounts = []
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            mnt = os.path.join(self.tmpdir, 'mnt.{id}'.format(id=id_))
            os.mkdir(mnt)
            ceph_sbindir = os.path.join(self.bindir, 'usr/local/sbin')

            mons = []
            for idx, roles in enumerate(self.all_roles):
                for role in roles:
                    if not role.startswith('mon.'):
                        continue
                    mon_id = int(role[len('mon.'):])
                    addr = '{ip}:{port}'.format(
                        ip=self.all_ips[idx],
                        port=6789+mon_id,
                        )
                    mons.append(addr)
            assert mons

            secret = utils.run(
                '{bindir}/cauthtool client.{id}.keyring -c {conf} --name=client.{id} -p'.format(
                    bindir=self.ceph_bindir,
                    conf=self.ceph_conf.filename,
                    id=id_,
                    ),
                verbose=False,
                )
            secret = secret.stdout.rstrip('\n')

            # the arguments MUST be in this order
            utils.system('{sbindir}/mount.ceph {mons}:/ {mnt} -v -o name={id},secret={secret}'.format(
                    sbindir=ceph_sbindir,
                    mons=','.join(mons),
                    mnt=mnt,
                    id=id_,
                    secret=secret,
                    ),
                )
            self.mounts.append(mnt)

    @skeleton.role('client')
    def do_150_ceph_fsx(self):
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            # TODO parallel?
            mnt = os.path.join(self.tmpdir, 'mnt.{id}'.format(id=id_))

            self.job.run_test(
                'fsx',
                dir=mnt,
                )

            print 'ceph fsx test ok'

    @skeleton.role('client')
    def do_190_ceph_unmount(self):
        for mnt in self.mounts:
            utils.system('umount {mnt}'.format(mnt=mnt))
