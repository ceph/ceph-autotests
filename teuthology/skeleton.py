import configobj
import functools
import gevent.server
import gevent.event
import os
import signal
import stat

from autotest_lib.client.bin import test
from autotest_lib.client.bin import utils

from .rpc import server
from .rpc import client
from . import ceph

RPC_PORT = 51991 # 0xCEFF ;)

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

client_config_defaults = {
    'rbd_kernel_mount' : True,
    'rbd_create'       : True,
    'rbd_fs'           : None,
    'rbd_size'         : 4096,
    }

class DaemonNotRunningError(Exception):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        return 'Daemon {role} is not running.'.format(**self.kwargs)

class CephTest(test.test):
    def run_hooks(self, prefix):
        hooks = sorted(name for name in dir(self) if name.startswith('{prefix}_'.format(prefix=prefix)))
        for name in hooks:
            print 'Running %s' % name
            fn = getattr(self, name)
            fn()

    def initialize(self, **kwargs):
        print 'Entering tmp directory:', self.tmpdir
        os.chdir(self.tmpdir)

        self.ceph_bin_url = kwargs.pop('ceph_bin_url', None)
        self.number = kwargs.pop('number')
        self.all_roles = kwargs.pop('all_roles')
        self.all_ips = kwargs.pop('all_ips')
        self.my_roles = self.all_roles[self.number]
        kwargs.setdefault('client_types', {})
        self.client_types = kwargs.pop('client_types')
        kwargs.setdefault('client_configs', {})
        self.client_configs = kwargs.pop('client_configs')
        self.cookie = kwargs.pop('cookie')
        self.extra = kwargs

        self.ceph_bindir = os.path.join(self.bindir, 'usr/local/bin')
        self.ceph_pydir = os.path.join(self.bindir,
            'usr/local/lib/python2.6/dist-packages')
        self.ceph_libdir = os.path.join(self.bindir, 'usr/local/lib')
        self.daemons = []
        # map role -> daemon on the node that actually has the process
        self.daemons_from_rpc = {}

        # map role -> greenlet running the daemon on mon.0
        self.daemons_via_rpc = {}

        # variables used to communicate between rpc methods and
        # autotest functions; these need to be set before the rpc
        # server is started, or we might get a call that tries to use
        # them before we initialize them
        self.mon0_info = gevent.event.AsyncResult()
        self.wait_healthy = gevent.event.AsyncResult()

        handler = server.Handler(
            cookie=self.cookie,
            lookup=self.get_rpc_method,
            )
        self.server = gevent.server.StreamServer(
            listener=('0.0.0.0', RPC_PORT),
            handle=handler,
            )
        self.server.start()

        self.run_hooks(prefix='init')

    def get_rpc_method(self, name):
        return getattr(self, 'rpc_{name}'.format(name=name), None)

    def run_once(self):
        print 'Entering tmp directory:', self.tmpdir
        os.chdir(self.tmpdir)

        self.run_hooks(prefix='do')

    def postprocess(self):
        print 'CWD', os.getcwd()
        print 'Entering tmp directory:', self.tmpdir
        os.chdir(self.tmpdir)

        self.run_hooks(prefix='hook_postprocess')

        self.server.stop()

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
            '{bindir}/cauthtool client.{id}.keyring -c ceph.conf --name=client.{id} -p'.format(
                bindir=self.ceph_bindir,
                id=id_,
                ),
            verbose=False,
            )
        secret = secret.stdout.rstrip('\n')
        return secret

    def generate_caps(self, role, id_):
        defaults = {
            'osd': '--cap mon "allow *" --cap osd "allow *"',
            'mds': '--cap mon "allow *" --cap osd "allow *" --cap mds "allow"',
            'client' : '--cap mon "allow rw" --cap osd "allow rwx pool=data,rbd" --cap mds "allow"'
            }
        return self.client_configs.get("{role}.{id_}".format(id_=id_,role=role),{}).get("caps",defaults.get(role, ""))

    def generate_tag_for_subjob(self, client_id):
        """
        Generate a unique tag suitable for running a subjob via
        self.job.run_test.

        Guaranteed to be unique across all concurrent host groups, for
        all current clients, and for every iteration.
        """
        assert '.' in self.tagged_testname, \
            "Test name must have tag: %r" % self.tagged_testname
        cluster = self.tagged_testname.split('.', 1)[1]
        tag = '{cluster}.client{client}.iter{iter}'.format(
            cluster=cluster,
            client=client_id,
            iter=self.iteration,
            )
        return tag

    def copy_subjob_results_kv(self, client_id, subjob_name):
        # i can't find a really nice way to construct this path
        results_path = os.path.join(
            self.outputdir,
            '..',
            '{name}.{tag}'.format(
                name=subjob_name,
                tag=self.generate_tag_for_subjob(client_id),
                ),
            'results',
            'keyval',
            )
        # TODO bleh read_keyval doesn't understand {perf}, and the tko
        # parsing is not going to be available on the client
        # kv = utils.read_keyval(results_dir)
        def read_keyval_iterations(path):
            def _read_iteration(f):
                for line in f:
                    assert line.endswith('\n')
                    line = line.rstrip('\n')

                    # TODO we don't support comments
                    assert '#' not in line

                    if not line:
                        # preserve iteration boundaries
                        return

                    k,v = line.split('=', 1)
                    # TODO kludge
                    assert k.endswith('{perf}')
                    k = k[:-len('{perf}')]
                    try:
                        v = int(v)
                    except ValueError:
                        try:
                            v = float(v)
                        except ValueError:
                            pass
                    yield (k,v)


            with file(path) as f:
                kv = dict(_read_iteration(f))
                yield kv

        # this is safe because we run the underlying job one iteration
        # at a time only, even if we ourselves have multiple
        # iterations
        g = read_keyval_iterations(path=results_path)
        for kv in g:
            role = 'client.{id}'.format(id=client_id)
            client_type = self.client_types.get(role, 'kclient')
            self.write_iteration_keyval(
                attr_dict=dict(
                    client_id=client_id,
                    client_type=client_type,
                    ),
                perf_dict=kv,
                )

    def init_010_announce(self):
        print 'This is host #%d with roles %s...' % (self.number, self.my_roles)

    def init_011_record(self):
        self.write_test_keyval(dict(
                num_host=len(self.all_roles),
                num_mon=num_instances_of_type(self.all_roles, 'mon'),
                num_mds=num_instances_of_type(self.all_roles, 'mds'),
                num_osd=num_instances_of_type(self.all_roles, 'osd'),
                num_client=num_instances_of_type(self.all_roles, 'client'),
                ))

    def init_012_get_binaries(self):
        ceph.get_binaries(self, self.ceph_bin_url)

    def init_015_symlink_results(self):
        # let ceph.conf use fixed pathnames
        os.symlink(self.resultsdir, 'results')
        os.mkdir('results/log')
        os.mkdir('results/profiling-logger')

    def init_015_dev(self):
        os.mkdir('dev')

    @role('osd')
    def init_015_class_tmp(self):
        os.mkdir('class_tmp')

    @role('mon.0')
    def init_019_clients(self):
        self.clients = [
            client.Client(
                address=(ip, RPC_PORT),
                cookie=self.cookie,
                )
            for ip in self.all_ips
            ]

    @role('mon.0')
    def init_020_ceph_conf(self):
        conf = ceph.skeleton_config()

        mons = self.get_mons()
        for role, addr in mons.iteritems():
            conf.setdefault(role, {})
            conf[role]['mon addr'] = addr

        for idx, roles in enumerate(self.all_roles):
            conf2 = configobj.ConfigObj()
            conf2.merge(conf)

            for id_ in roles_of_type(roles, 'client'):
                section = 'client.{id}'.format(id=id_)
                conf2.setdefault(section, {})
                conf2[section]['keyring'] = 'client.{id}.keyring'.format(id=id_)

            g = self.clients[idx].call('set_ceph_conf', conf=conf2.dict())
            g.get()

    def rpc_set_ceph_conf(self, conf):
        # conf should be a dict-of-dicts
        o = configobj.ConfigObj()
        o.merge(conf)
        o.filename = os.path.join(self.tmpdir, 'ceph.conf')
        o.write()

    @role('mon.0')
    def init_030_create_keyring(self):
        utils.system('{bindir}/cauthtool --create-keyring ceph.keyring'.format(
                bindir=self.ceph_bindir,
                ))

    @role('mon.0')
    def init_031_generate_mon0_key(self):
        utils.system('{bindir}/cauthtool --gen-key --name=mon. ceph.keyring'.format(
                bindir=self.ceph_bindir,
                ))

    @role('mon.0')
    def init_031_generate_admin_key(self):
        utils.system('{bindir}/cauthtool --gen-key --name=client.admin --set-uid=0 --cap mon "allow *" --cap osd "allow *" --cap mds "allow" ceph.keyring'.format(
                bindir=self.ceph_bindir,
                ))

    @role('mon.0')
    def init_033_generate_monmap(self):
        ceph.create_simple_monmap(self)

    def rpc_receive_mon0_info(self, key, monmap):
        with file(os.path.join(self.tmpdir, 'ceph.keyring'), 'w') as f:
            f.write(key)
        # decode monmap because json can't transport binary
        monmap = monmap.decode('base64')
        with file(os.path.join(self.tmpdir, 'monmap'), 'w') as f:
            f.write(monmap)
        self.mon0_info.set(None)

    @role('mon.0')
    def init_035_ship_mon0_info(self):
        key = file('ceph.keyring').read()
        # encode monmap so it can be transported in json
        monmap = file('monmap').read().encode('base64')

        for idx, roles in enumerate(self.all_roles):
            for id_ in roles_of_type(roles, 'mon'):
                if id_ == '0':
                    continue

                # copy mon key and initial monmap
                print 'Sending mon0 info to node {idx}'.format(idx=idx)
                g = self.clients[idx].call(
                    'receive_mon0_info',
                    key=key,
                    monmap=monmap,
                    )
                # TODO run in parallel
                g.get()

                # no need to do more than once per host
                break

        self.mon0_info.set()

    @role('mon')
    def init_036_wait_mon0_info(self):
        # wait until the rpc has been called
        self.mon0_info.get()

    @role('mon')
    def init_041_daemons_mon_osdmap(self):
        utils.system('{bindir}/osdmaptool --clobber --createsimple {num_osd} osdmap --pg_bits 2 --pgp_bits 4'.format(
                num_osd=num_instances_of_type(self.all_roles, 'osd'),
                bindir=self.ceph_bindir,
                ))

    @role('mon')
    def init_042_daemons_mon_mkfs(self):
        for id_ in roles_of_type(self.my_roles, 'mon'):
            utils.system('{bindir}/cmon --mkfs -i {id} -c ceph.conf --monmap=monmap --osdmap=osdmap --keyring=ceph.keyring'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    ))

    @role('mon')
    def init_045_daemons_mon_start(self):
        for id_ in roles_of_type(self.my_roles, 'mon'):
            proc = utils.BgJob(command='{bindir}/cmon -f -i {id} -c ceph.conf'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    ))
            self.daemons.append(proc)

    @role('mon')
    def init_049_daemons_mon_monmap_delete(self):
        os.unlink('monmap')

    @role('mon')
    def init_049_daemons_mon_osdmap_delete(self):
        os.unlink('osdmap')

    @role('osd')
    def init_050_generate_key_osd(self):
        for id_ in roles_of_type(self.my_roles, 'osd'):
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=osd.{id} dev/osd.{id}.keyring'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    ))

    @role('mds')
    def init_050_generate_key_mds(self):
        for id_ in roles_of_type(self.my_roles, 'mds'):
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=mds.{id} dev/mds.{id}.keyring'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    ))

    @role('client')
    def init_050_generate_key_client(self):
        for id_ in roles_of_type(self.my_roles, 'client'):
            # TODO this --name= is not really obeyed, all unknown "types" are munged to "client"
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=client.{id} client.{id}.keyring'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    ))


    def init_055_key_shuffle(self):
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
            for type_ in ['osd','mds','client']:
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
                                caps=self.generate_caps(type_, id_),
                                ))
                        utils.system('{bindir}/ceph -c ceph.conf -k ceph.keyring -i temp.keyring auth add {type}.{id}'.format(
                                bindir=self.ceph_bindir,
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
    def init_056_set_max_mds(self):
        # TODO where does this belong?
        utils.system('{bindir}/ceph -c ceph.conf -k ceph.keyring mds set_max_mds {num_mds}'.format(
                bindir=self.ceph_bindir,
                num_mds=num_instances_of_type(self.all_roles, 'mds'),
                ))

    def rpc_mkfs_osd(self, id_):
        role = 'osd.{id}'.format(id=id_)
        assert role in self.my_roles
        os.mkdir(os.path.join('dev', 'osd.{id}.data'.format(id=id_)))
        p = subprocess.Popen(
            args=[
                os.path.join(self.ceph_bindir, 'cosd'),
                '--mkfs',
                '-i', str(id_),
                '-c', 'ceph.conf'
                ],
            close_fds=True,
            cwd=self.tmpdir,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            )
        (out, err) = p.communicate()
        for line in out.splitlines():
            log.debug('osd mkfs stdout: %s', line)
        for line in err.splitlines():
            log.warning('osd mkfs stderr: %s', line)
        assert p.returncode is not None
        if p.returncode != 0:
            raise RuntimeError('osd mkfs failed with exit status %r' % p.returncode)

    def rpc_run_osd(self, id_):
        role = 'osd.{id}'.format(id=id_)
        assert role in self.my_roles
        print 'Starting daemon %r' % role
        proc = utils.BgJob(command='{bindir}/cosd -f -i {id} -c ceph.conf'.format(
                bindir=self.ceph_bindir,
                id=id_,
                ))
        assert role not in self.daemons_from_rpc
        self.daemons_from_rpc[role] = proc
        utils.join_bg_jobs([proc])
        p2 = self.daemons_from_rpc.pop(role)
        assert p2 is proc
        assert proc.result.exit_status is not None
        print 'Daemon %r exited with %r' % (role, proc.result.exit_status)
        return proc.result.exit_status

    def rpc_terminate_osd(self, id_):
        role = 'osd.{id}'.format(id=id_)
        assert role in self.my_roles
        proc = self.daemons_from_rpc.get(role)
        if proc is None:
            raise DaemonNotRunningError(role=role)
        proc.sp.terminate()

    @role('mon.0')
    def init_062_osd_start(self):
        for idx, roles in enumerate(self.all_roles):
            for id_ in roles_of_type(roles, 'osd'):
                role = 'osd.{id}'.format(id=id_)

                print 'mkfs osd on node {idx}'.format(idx=idx)
                g = self.clients[idx].call(
                    'mkfs_osd',
                    id_=id_,
                    )
                g.get()

                print 'Running osd on node {idx}'.format(idx=idx)
                g = self.clients[idx].call(
                    'run_osd',
                    id_=id_,
                    )
                assert role not in self.daemons_via_rpc
                self.daemons_via_rpc[role] = g

    @role('mds')
    def init_063_mds_start(self):
        for id_ in roles_of_type(self.my_roles, 'mds'):
            proc = utils.BgJob(command='{bindir}/cmds -f -i {id} -c ceph.conf'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    ))
            self.daemons.append(proc)

    @role('mon.0')
    def init_065_wait_healthy(self):
        # others wait for rpc from us
        ceph.wait_until_healthy(self)

        for idx in range(len(self.all_roles)):
            # copy mon key and initial monmap
            print 'Telling node {idx} cluster is healthy'.format(idx=idx)
            g = self.clients[idx].call('set_healthy')
            # TODO run in parallel
            g.get()

    def rpc_set_healthy(self):
        self.wait_healthy.set()

    def init_069_barrier_healthy(self):
        # wait until notified that cluster is healthy
        self.wait_healthy.get()

    def client_is_type(self, id_, type_):
        """
        Use the given type for mounting client with given id, or not?
        """
        role = 'client.{id}'.format(id=id_)
        return type_ == self.client_types.get(role, 'kclient')

    def get_client_config(self, id_, key):
        role = 'client.{id}'.format(id=id_)
        return self.client_configs.get(role,{}).get(key, client_config_defaults[key])

    @role('client')
    def init_071_cfuse_mount(self):
        self.fuses = []
        for id_ in roles_of_type(self.my_roles, 'client'):
            if not self.client_is_type(id_, 'cfuse'):
                continue
            mnt = os.path.join(self.tmpdir, 'mnt.{id}'.format(id=id_))
            os.mkdir(mnt)
            fuse = utils.BgJob(
                # we could use -m instead of ceph.conf, but as we need
                # ceph.conf to find the keyring anyway, it's not yet worth it

                command='{bindir}/cfuse -f -c ceph.conf --name=client.{id} {mnt}'.format(
                    bindir=self.ceph_bindir,
                    id=id_,
                    mnt=mnt,
                    ),
                stdout_tee=utils.TEE_TO_LOGS,
                stderr_tee=utils.TEE_TO_LOGS,
                )
            self.fuses.append((mnt, fuse))
            ceph.wait_until_fuse_mounted(self, fuse=fuse, mountpoint=mnt)

    @role('client')
    def init_072_kernel_mount(self):
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

    @role('mon.0')
    def init_073_rbd_activate(self):
        if not 'rbd' in self.client_types.values():
            return

        rbd_file = os.path.join(self.ceph_libdir, 'rados-classes/libcls_rbd.so')
        cls_info = utils.run('{bindir}/cclsinfo {class_file}'.format(
                bindir=self.ceph_bindir,
                class_file=rbd_file,
                ))
        cls_info = cls_info.stdout.rstrip('\n')
        utils.system('{bindir}/ceph -c ceph.conf --name client.admin class add -i {rbd_file} {cls_info}'.format(
                bindir=self.ceph_bindir,
                rbd_file=rbd_file,
                cls_info=cls_info,
                ))
        utils.system('{bindir}/ceph -c ceph.conf --name client.admin class activate rbd {rbd_version}'.format(
                bindir=self.ceph_bindir,
                rbd_version='1.3',
                ))

    @role('mon.0')
    def init_074_create_rbd(self):
        for roles in self.all_roles:
            for id_ in roles_of_type(roles, 'client'):
                if not (self.client_is_type(id_, 'rbd') and
                        self.get_client_config(id_, 'rbd_create')):
                    continue

                LD_LIB = os.getenv('LD_LIBRARY_PATH', '')
                if self.ceph_libdir not in LD_LIB:
                    os.putenv('LD_LIBRARY_PATH', LD_LIB + ':' + self.ceph_libdir)
                utils.run('{bindir}/rbd create -s {size} {name}'.format(
                        bindir=self.ceph_bindir,
                        size=self.get_client_config(id_, 'rbd_size'),
                        name='testimage{id}'.format(id=id_),
                        ))

    def init_075_barrier_rbd_created(self):
        if not 'rbd' in self.client_types.values():
            return
        # rbd images have been created
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in self.all_ips]
        self.job.barrier(
            hostid=barrier_ids[self.number],
            tag='rbd_images_created',
            ).rendezvous(*barrier_ids)

    @role('client')
    def init_076_rbd_modprobe(self):
        for id_ in roles_of_type(self.my_roles, 'client'):
            if self.client_is_type(id_, 'rbd') and \
                    self.get_client_config(id_, 'rbd_kernel_mount'):
                utils.run('modprobe rbd')
                return

    @role('client')
    def init_077_rbd_dev_create(self):
        self.rbd_dev_ids = {}
        for id_ in roles_of_type(self.my_roles, 'client'):
            if not (self.client_is_type(id_, 'rbd') and
                    self.get_client_config(id_, 'rbd_kernel_mount')):
                continue

            image_name = 'testimage{id}'.format(id=id_)
            secret = self.get_secret(id_)

            with open('/sys/bus/rbd/add', 'w') as add_file:
                add_file.write('{mons} name={name},secret={secret} rbd {image}'.format(
                        mons=','.join(self.get_mons().values()),
                        name=id_,
                        secret=secret,
                        image=image_name,
                        ),
                    )

            basepath = '/sys/bus/rbd/devices'
            for dev_id in os.listdir(basepath):
                devpath = os.path.join(basepath, dev_id)
                name = utils.run('cat {file}'.format(file=os.path.join(devpath, 'name')))
                name = name.stdout.rstrip('\n')
                major = utils.run('cat {file}'.format(file=os.path.join(devpath, 'major')))
                major = int(major.stdout.rstrip('\n'))

                if name == image_name:
                    try:
                        os.stat('/dev/rbd')
                    except OSError as err:
                        import errno
                        assert(err.errno == errno.ENOENT)
                        os.mkdir('/dev/rbd')

                    os.mknod('/dev/rbd/{image}'.format(image=image_name),
                             0600 | stat.S_IFBLK,
                             os.makedev(major, 0),
                             )
                    self.rbd_dev_ids[image_name] = dev_id

    @role('client')
    def init_078_rbd_preparefs(self):
        for id_ in roles_of_type(self.my_roles, 'client'):
            if not (self.client_is_type(id_, 'rbd') and
                    self.get_client_config(id_, 'rbd_fs') is not None):
                continue
            image_name = 'testimage{id}'.format(id=id_)
            utils.system('mkfs -t {fs} /dev/rbd/{image}'.format(
                    fs=self.get_client_config(id_, 'rbd_fs'),
                    image=image_name,
                    ))

    @role('client')
    def init_079_rbd_mount(self):
        for id_ in roles_of_type(self.my_roles, 'client'):
            if not (self.client_is_type(id_, 'rbd') and
                    self.get_client_config(id_, 'rbd_kernel_mount') and
                    self.get_client_config(id_, 'rbd_fs') is not None):
                continue
            image_name = 'testimage{id}'.format(id=id_)
            mnt = os.path.join(self.tmpdir, image_name)
            os.mkdir(mnt)
            utils.system('mount -t {fs} /dev/rbd/{image} {mnt}'.format(image=image_name, mnt=mnt, fs=self.get_client_config(id_, 'rbd_fs')))

    @role('client')
    def hook_postprocess_901_cfuse_unmount(self):
        for mnt, fuse in self.fuses:
            utils.system('fusermount -u {mnt}'.format(mnt=mnt))
            print 'Waiting for cfuse to exit...'
            utils.join_bg_jobs([fuse])
            assert fuse.result.exit_status == 0, \
                'cfuse failed with: %r' % fuse.result.exit_status

    @role('client')
    def hook_postprocess_902_kernel_unmount(self):
        for mnt in self.mounts:
            utils.system('umount {mnt}'.format(mnt=mnt))

    @role('client')
    def hook_postprocess_903_rbd_kernel_unmount(self):
        kernel_mounted = False
        for id_ in roles_of_type(self.my_roles, 'client'):
            if not (self.client_is_type(id_, 'rbd') and
                    self.get_client_config(id_, 'rbd_kernel_mount')):
                continue
            kernel_mounted = True
            image_name = 'testimage{id}'.format(id=id_)
            mnt = os.path.join(self.tmpdir, image_name)
            utils.system('umount {path}'.format(path=mnt))
            os.rmdir(mnt)
            os.remove('/dev/rbd/{image}'.format(image=image_name))

        if kernel_mounted:
            os.rmdir('/dev/rbd')

    @role('client')
    def hook_postprocess_904_rbd_dev_remove(self):
        for dev_id in self.rbd_dev_ids.itervalues():
            with open('/sys/bus/rbd/remove', 'w') as rem_file:
                rem_file.write(dev_id)

    def hook_postprocess_910_barrier_done(self):
        # wait until client is done
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in self.all_ips]
        self.job.barrier(
            hostid=barrier_ids[self.number],
            tag='done',
            ).rendezvous(*barrier_ids)

    @role('mon.0')
    def hook_postprocess_940_daemon_shutdown_rpc(self):
        for idx, roles in enumerate(self.all_roles):
            for id_ in roles_of_type(roles, 'osd'):
                role = 'osd.{id}'.format(id=id_)
                g = self.daemons_via_rpc.pop(role, None)
                if g is None:
                    continue
                stop = self.clients[idx].call(
                    'terminate_osd',
                    id_=id_,
                    )
                try:
                    stop.get()
                except client.RPCError as e:
                    if e.code == 'DaemonNotRunningError':
                        pass
                    else:
                        raise
                status = g.get()
                assert status in [0, -signal.SIGTERM], \
                    'daemon %r failed with: %r' % (role, status)

        assert not self.daemons_via_rpc

    def hook_postprocess_950_daemon_shutdown(self):
        for d in self.daemons:
            d.sp.terminate()
        utils.join_bg_jobs(self.daemons)
        for d in self.daemons:
            # TODO daemons should catch sigterm and exit 0
            assert d.result.exit_status in [0, -signal.SIGTERM], \
                'daemon %r failed with: %r' % (d.result.command, d.result.exit_status)
