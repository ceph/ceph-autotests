import functools
import os
import signal

from autotest_lib.client.bin import test
from autotest_lib.client.bin import utils

from . import ceph

def roles_of_type(my_roles, type_):
    prefix = '{type}.'.format(type=type_)
    for name in my_roles:
        if not name.startswith(prefix):
            continue
        id_ = name[len(prefix):]
        yield id_

def num_instances_of_type(all_roles, type_):
    prefix = '{type}.'.format(type=type_)
    num = sum(sum(1 for role in hostroles if role.startswith(prefix)) for hostroles in all_roles)
    return num

def server_with_role(all_roles, role):
    for idx, host_roles in enumerate(all_roles):
        if role in host_roles:
            return idx

def role(*roles_or_types):
    """
    Only run this hook for the listed hooks or types.

    For example::

	@role('osd.2', 'osd.3')
	@role('mds')
    """

    def should_run(my_roles):
        for role_or_type in roles_or_types:
            if '.' in role_or_type:
                # it's a role
                if role_or_type in my_roles:
                    return True
            else:
                # it's a type
                prefix = '{type}.'.format(type=role_or_type)
                if any(role.startswith(prefix) for role in my_roles):
                    return True
        return False

    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            if should_run(self.my_roles):
                return fn(self, *args, **kwargs)
        return wrapper

    return decorate


class CephTest(test.test):
    def setup(self, **kwargs):
        ceph.get_binaries(self, kwargs.get('ceph_bin_url'))

    def run_once(self, **kwargs):
        self.number = kwargs.pop('number')
        self.all_roles = kwargs.pop('all_roles')
        self.all_ips = kwargs.pop('all_ips')
        self.my_roles = self.all_roles[self.number]
        kwargs.setdefault('client_types', {})
        self.client_types = kwargs.pop('client_types')
        self.extra = kwargs

        self.ceph_bindir = os.path.join(self.bindir, 'usr/local/bin')
        self.daemons = []

        print 'Entering tmp directory:', self.tmpdir
        os.chdir(self.tmpdir)

        self.run_hooks()

    def run_hooks(self):
        hooks = sorted(name for name in dir(self) if name.startswith('do_'))
        for name in hooks:
            print 'Running %s' % name
            fn = getattr(self, name)
            fn()

    def get_mons(self):
        mons = {}
        for idx, roles in enumerate(self.all_roles):
            for role in roles:
                if not role.startswith('mon.'):
                    continue
                mon_id = int(role[len('mon.'):])
                addr = '{ip}:{port}'.format(
                    ip=self.all_ips[idx],
                    port=6789+mon_id,
                    )
                mons[role] = addr
        assert mons
        return mons

    def get_secret(self, id_):
        secret = utils.run(
            '{bindir}/cauthtool client.{id}.keyring -c {conf} --name=client.{id} -p'.format(
                bindir=self.ceph_bindir,
                conf=self.ceph_conf.filename,
                id=id_,
                ),
            verbose=False,
            )
        secret = secret.stdout.rstrip('\n')
        return secret

    def do_010_announce(self):
        print 'This is host #%d with roles %s...' % (self.number, self.my_roles)

    def do_015_symlink_results(self):
        # let ceph.conf use fixed pathnames
        os.symlink(self.resultsdir, 'results')
        os.mkdir('results/log')
        os.mkdir('results/profiling-logger')

    def do_015_dev(self):
        os.mkdir('dev')

    def do_020_conf_create(self):
        self.ceph_conf = ceph.skeleton_config(self)

    def do_021_conf_add_mons(self):
        mons = self.get_mons()
        for role, addr in mons.iteritems():
            self.ceph_conf.setdefault(role, {})
            self.ceph_conf[role]['mon addr'] = addr

    @role('client')
    def do_025_conf_client_keyring(self):
        for id_ in roles_of_type(self.my_roles, 'client'):
            section = 'client.{id}'.format(id=id_)
            self.ceph_conf.setdefault(section, {})
            self.ceph_conf[section]['keyring'] = 'client.{id}.keyring'.format(id=id_)

    def do_029_conf_write(self):
        self.ceph_conf.write()
        print 'Wrote config to', self.ceph_conf.filename

    @role('mon.0')
    def do_030_create_keyring(self):
        utils.system('{bindir}/cauthtool --create-keyring ceph.keyring'.format(
                bindir=self.ceph_bindir,
                ))

    @role('mon.0')
    def do_031_generate_mon0_key(self):
        utils.system('{bindir}/cauthtool --gen-key --name=mon. ceph.keyring'.format(
                bindir=self.ceph_bindir,
                ))

    @role('mon.0')
    def do_031_generate_admin_key(self):
        utils.system('{bindir}/cauthtool --gen-key --name=client.admin --set-uid=0 --cap mon "allow *" --cap osd "allow *" --cap mds "allow" ceph.keyring'.format(
                bindir=self.ceph_bindir,
                ))

    @role('mon.0')
    def do_033_generate_monmap(self):
        ceph.create_simple_monmap(self)

    @role('mon.0')
    def do_035_export_mon0_info(self):
        # export mon. key
        self.mon0_serve = utils.BgJob(command='env PYTHONPATH={at_bindir} python -m teuthology.ceph_serve_file --port=11601 --publish=/mon0key:ceph.keyring --publish=/monmap:monmap'.format(
                at_bindir=self.bindir,
                ))

    @role('mon')
    def do_036_import_mon0_info(self):
        idx_of_mon0 = server_with_role(self.all_roles, 'mon.0')
        for id_ in roles_of_type(self.my_roles, 'mon'):
            if id_ == '0':
                continue

            # copy mon key
            ceph.urlretrieve_retry(
                url='http://{ip}:11601/mon0key'.format(ip=self.all_ips[idx_of_mon0]),
                filename='ceph.keyring',
                )

            # copy initial monmap
            ceph.urlretrieve_retry(
                url='http://{ip}:11601/monmap'.format(ip=self.all_ips[idx_of_mon0]),
                filename='monmap',
                )

            # no need to do more than once per host
            break

    def do_038_barrier_mon0_info(self):
        # mon.0 is now exporting its data, wait until mon.N has copied it
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in self.all_ips]
        self.job.barrier(
            hostid=barrier_ids[self.number],
            tag='mon0_copied',
            ).rendezvous(*barrier_ids)

    @role('mon.0')
    def do_039_export_mon0_info_stop(self):
        mon0_serve = self.mon0_serve
        del self.mon0_serve
        mon0_serve.sp.terminate()
        utils.join_bg_jobs([mon0_serve])
        assert mon0_serve.result.exit_status in [0, -signal.SIGTERM], \
            'mon.0 key serving failed with: %r' % mon0_serve.result.exit_status

    @role('mon')
    def do_041_daemons_mon_osdmap(self):
        utils.system('{bindir}/osdmaptool --clobber --createsimple {num_osd} osdmap --pg_bits 2 --pgp_bits 4'.format(
                num_osd=num_instances_of_type(self.all_roles, 'osd'),
                bindir=self.ceph_bindir,
                conf=self.ceph_conf.filename,
                ))

    @role('mon')
    def do_042_daemons_mon_mkfs(self):
        for id_ in roles_of_type(self.my_roles, 'mon'):
            utils.system('{bindir}/cmon --mkfs -i {id} -c {conf} --monmap=monmap --osdmap=osdmap --keyring=ceph.keyring'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))

    @role('mon')
    def do_045_daemons_mon_start(self):
        for id_ in roles_of_type(self.my_roles, 'mon'):
            proc = utils.BgJob(command='{bindir}/cmon -f -i {id} -c {conf}'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            self.daemons.append(proc)

    @role('mon')
    def do_049_daemons_mon_monmap_delete(self):
        os.unlink('monmap')

    @role('mon')
    def do_049_daemons_mon_osdmap_delete(self):
        os.unlink('osdmap')

    @role('osd')
    def do_050_generate_key_osd(self):
        for id_ in roles_of_type(self.my_roles, 'osd'):
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=osd.{id} dev/osd.{id}.keyring'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    ))

    @role('mds')
    def do_050_generate_key_mds(self):
        for id_ in roles_of_type(self.my_roles, 'mds'):
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=mds.{id} dev/mds.{id}.keyring'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    ))

    @role('client')
    def do_050_generate_key_client(self):
        for id_ in roles_of_type(self.my_roles, 'client'):
            # TODO this --name= is not really obeyed, all unknown "types" are munged to "client"
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=client.{id} client.{id}.keyring'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    ))


    def do_055_key_shuffle(self):
        # copy keys to mon.0
        publish = []
        for id_ in roles_of_type(self.my_roles, 'osd'):
            publish.append('--publish=/key/osd.{id}.keyring:dev/osd.{id}.keyring'.format(id=id_))
        for id_ in roles_of_type(self.my_roles, 'mds'):
            publish.append('--publish=/key/mds.{id}.keyring:dev/mds.{id}.keyring'.format(id=id_))
        for id_ in roles_of_type(self.my_roles, 'client'):
            publish.append('--publish=/key/client.{id}.keyring:client.{id}.keyring'.format(id=id_))
        key_serve = utils.BgJob(command='env PYTHONPATH={at_bindir} python -m teuthology.ceph_serve_file --port=11601 {publish}'.format(
                at_bindir=self.bindir,
                publish=' '.join(publish),
                ))

        if 'mon.0' in self.my_roles:
            for type_, caps in [
                ('osd', '--cap mon "allow *" --cap osd "allow *"'),
                ('mds', '--cap mon "allow *" --cap osd "allow *" --cap mds "allow"'),
                ('client', '--cap mon "allow r" --cap osd "allow rw pool=data" --cap mds "allow"'),
                ]:
                for idx, host_roles in enumerate(self.all_roles):
                    print 'Fetching {type} keys from host {idx} ({ip})...'.format(
                        type=type_,
                        idx=idx,
                        ip=self.all_ips[idx],
                        )
                    for id_ in roles_of_type(host_roles, type_):
                        ceph.urlretrieve_retry(
                            url='http://{ip}:11601/key/{type}.{id}.keyring'.format(
                                ip=self.all_ips[idx],
                                type=type_,
                                id=id_,
                                ),
                            filename='temp.keyring',
                            )
                        utils.system('{bindir}/cauthtool temp.keyring --name={type}.{id} {caps}'.format(
                                bindir=self.ceph_bindir,
                                type=type_,
                                id=id_,
                                caps=caps,
                                ))
                        utils.system('{bindir}/ceph -c {conf} -k ceph.keyring -i temp.keyring auth add {type}.{id}'.format(
                                bindir=self.ceph_bindir,
                                conf=self.ceph_conf.filename,
                                type=type_,
                                id=id_,
                                ))

        # wait until osd/mds/client keys have been copied and authorized
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in self.all_ips]
        self.job.barrier(
            hostid=barrier_ids[self.number],
            tag='authorized',
            ).rendezvous(*barrier_ids)
        key_serve.sp.terminate()
        utils.join_bg_jobs([key_serve])
        assert key_serve.result.exit_status in [0, -signal.SIGTERM], \
            'general key serving failed with: %r' % key_serve.result.exit_status

    @role('mon.0')
    def do_056_set_max_mds(self):
        # TODO where does this belong?
        utils.system('{bindir}/ceph -c {conf} -k ceph.keyring mds set_max_mds {num_mds}'.format(
                bindir=self.ceph_bindir,
                conf=self.ceph_conf.filename,
                num_mds=num_instances_of_type(self.all_roles, 'mds'),
                ))

    @role('osd')
    def do_061_osd_mkfs(self):
        for id_ in roles_of_type(self.my_roles, 'osd'):
            os.mkdir(os.path.join('dev', 'osd.{id}.data'.format(id=id_)))
            utils.system('{bindir}/cosd --mkfs -i {id} -c {conf}'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))

    @role('osd')
    def do_062_osd_start(self):
        for id_ in roles_of_type(self.my_roles, 'osd'):
            proc = utils.BgJob(command='{bindir}/cosd -f -i {id} -c {conf}'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            self.daemons.append(proc)

    @role('mds')
    def do_063_mds_start(self):
        for id_ in roles_of_type(self.my_roles, 'mds'):
            proc = utils.BgJob(command='{bindir}/cmds -f -i {id} -c {conf}'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            self.daemons.append(proc)

    @role('mon.0')
    def do_065_wait_healthy(self):
        # others wait on barrier
        ceph.wait_until_healthy(self)

        utils.system('{bindir}/ceph -c {conf} -s'.format(
                bindir=self.ceph_bindir,
                conf=self.ceph_conf.filename,
                ))

    def do_069_barrier_healthy(self):
        # server is now healthy
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in self.all_ips]
        self.job.barrier(
            hostid=barrier_ids[self.number],
            tag='healthy',
            ).rendezvous(*barrier_ids)

    def client_is_type(self, id_, type_):
        """
        Use the given type for mounting client with given id, or not?
        """
        role = 'client.{id}'.format(id=id_)
        return type_ == self.client_types.get(role, 'kclient')

    @role('client')
    def do_071_cfuse_mount(self):
        self.fuses = []
        for id_ in roles_of_type(self.my_roles, 'client'):
            if not self.client_is_type(id_, 'cfuse'):
                continue
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

    @role('client')
    def do_072_kernel_mount(self):
        self.mounts = []
        for id_ in roles_of_type(self.my_roles, 'client'):
            if not self.client_is_type(id_, 'kclient'):
                continue
            mnt = os.path.join(self.tmpdir, 'mnt.{id}'.format(id=id_))
            os.mkdir(mnt)
            ceph_sbindir = os.path.join(self.bindir, 'usr/local/sbin')

            mons = self.get_mons().values()
            secret = self.get_secret(id_)

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

    @role('client')
    def do_901_cfuse_unmount(self):
        for mnt, fuse in self.fuses:
            utils.system('fusermount -u {mnt}'.format(mnt=mnt))
            print 'Waiting for cfuse to exit...'
            utils.join_bg_jobs([fuse])
            assert fuse.result.exit_status == 0, \
                'cfuse failed with: %r' % fuse.result.exit_status

    @role('client')
    def do_902_kernel_unmount(self):
        for mnt in self.mounts:
            utils.system('umount {mnt}'.format(mnt=mnt))

    def do_910_barrier_done(self):
        # wait until client is done
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in self.all_ips]
        self.job.barrier(
            hostid=barrier_ids[self.number],
            tag='done',
            ).rendezvous(*barrier_ids)

    def do_950_daemon_shutdown(self):
        for d in self.daemons:
            d.sp.terminate()
        utils.join_bg_jobs(self.daemons)
        for d in self.daemons:
            # TODO daemons should catch sigterm and exit 0
            assert d.result.exit_status in [0, -signal.SIGTERM], \
                'daemon %r failed with: %r' % (d.result.command, d.result.exit_status)
