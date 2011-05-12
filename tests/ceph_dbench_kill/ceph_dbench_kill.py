import gevent
import os
import random
import shutil
import signal
import time

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton
from teuthology.rpc import client

class ceph_dbench_kill(skeleton.CephTest):
    version = 1

    @skeleton.role('mon.0')
    def init_100_start_killer(self):
        print 'INIT 100 CWD IS', os.getcwd()
        self.killer_done = False
        def killer():
            print 'KILLER CWD IS', os.getcwd()
            while not self.killer_done:
                max_num = skeleton.num_instances_of_type(self.all_roles, 'osd')
                victim = random.randrange(max_num)
                role = 'osd.{id}'.format(id=victim)
                print 'Killing daemon %r' % role
                assert role in self.daemons_via_rpc
                idx = skeleton.server_with_role(self.all_roles, role)
                g = self.clients[idx].call(
                    'terminate_osd',
                    id_=victim,
                    )
                try:
                    g.get()
                except client.RPCError as e:
                    if e.code == 'DaemonNotRunningError':
                        pass
                    else:
                        raise
                g = self.daemons_via_rpc.pop(role)
                status = g.get()
                assert status in [0, -signal.SIGTERM], \
                    'daemon %r failed with: %r' % (role, status)
                g = self.clients[idx].call(
                    'run_osd',
                    id_=victim,
                    )
                self.daemons_via_rpc[role] = g
                g.get()
                # avoid getting too anxious with the killing
                print 'CWD IS', os.getcwd()
                ceph.wait_until_healthy(self)
                time.sleep(60)

        self.killer_greenlet = gevent.spawn(killer)

    @skeleton.role('client')
    def do_150_ceph_dbench(self):
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            # TODO parallel?
            mnt = os.path.join(self.tmpdir, 'mnt.{id}'.format(id=id_))
            # a unique directory for each client
            client_dir = os.path.join(mnt, 'client.{id}'.format(id=id_))
            os.mkdir(client_dir)

            self.job.run_test(
                'dbench',
                dir=client_dir,
                tag=self.generate_tag_for_subjob(client_id=id_),
                )
            shutil.rmtree(client_dir)

            print 'ceph dbench test ok'

    def postprocess_iteration(self):
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            self.copy_subjob_results_kv(
                client_id=id_,
                subjob_name='dbench',
                )

    @skeleton.role('mon.0')
    def hook_postprocess_920_stop_killer(self):
        self.killer_done = True
        self.killer_greenlet.get()
