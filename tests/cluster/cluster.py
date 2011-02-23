import errno
import os
import socket
import urllib2

from autotest_lib.client.bin import test
from autotest_lib.client.bin import utils

from teuthology import ceph

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

def urlretrieve_retry(url, filename):
    # TODO handle ultimate timeout
    while True:
        try:
            utils.urlretrieve(url=url, filename=filename)
        except urllib2.URLError as e:
            args = getattr(e, 'args', ())
            if (args
                and isinstance(e.args[0], socket.error)
                and e.args[0].errno == errno.ECONNREFUSED):
                pass
            else:
                raise
        else:
            break

class cluster(test.test):
    version = 1

    def setup(self, **kwargs):
        ceph.get_binaries(self, kwargs.get('ceph_bin_url'))

    def run_once(self, **kwargs):
        number = kwargs['number']
        all_roles = kwargs['all_roles']
        all_ips = kwargs['all_ips']
        my_roles = all_roles[number]

        print 'This is host #%d with roles %s...' % (number, my_roles)
        print 'Entering tmp directory:', self.tmpdir
        os.chdir(self.tmpdir)

        # let ceph.conf use fixed pathnames
        os.symlink(self.resultsdir, 'results')
        os.mkdir('results/log')
        os.mkdir('results/profiling-logger')

        os.mkdir('dev')

        self.ceph_bindir = ceph_bin = os.path.join(self.bindir, 'usr/local/bin')

        self.ceph_conf = ceph.skeleton_config(self)

        for idx, roles in enumerate(all_roles):
            for role in roles:
                if not role.startswith('mon.'):
                    continue
                id_ = int(role[len('mon.'):])
                self.ceph_conf.setdefault(role, {})
                self.ceph_conf[role]['mon addr'] = '{ip}:{port}'.format(
                    ip=all_ips[idx],
                    port=6789+id_,
                    )

        for id_ in roles_of_type(my_roles, 'client'):
            section = 'client.{id}'.format(id=id_)
            self.ceph_conf.setdefault(section, {})
            self.ceph_conf[section]['keyring'] = 'client.{id}.keyring'.format(id=id_)

        self.ceph_conf.write()
        print 'Wrote config to', self.ceph_conf.filename

        if 'mon.0' in my_roles:
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=mon. ceph.keyring'.format(
                    bindir=ceph_bin,
                    ))

            utils.system('{bindir}/cauthtool --gen-key --name=client.admin --set-uid=0 --cap mon "allow *" --cap osd "allow *" --cap mds "allow" ceph.keyring'.format(
                    bindir=ceph_bin,
                    ))
            ceph.create_simple_monmap(self)

        if 'mon.0' in my_roles:
            # export mon. key
            mon0_serve = utils.BgJob(command='env PYTHONPATH={at_bindir} python -m teuthology.ceph_serve_file --port=11601 --publish=/mon0key:ceph.keyring --publish=/monmap:monmap'.format(
                                    at_bindir=self.bindir,
                                    ))

        idx_of_mon0 = server_with_role(all_roles, 'mon.0')
        for id_ in roles_of_type(my_roles, 'mon'):
            if id_ == '0':
                continue

            # copy mon key
            urlretrieve_retry(
                url='http://{ip}:11601/mon0key'.format(ip=all_ips[idx_of_mon0]),
                filename='ceph.keyring',
                )

            # copy initial monmap
            urlretrieve_retry(
                url='http://{ip}:11601/monmap'.format(ip=all_ips[idx_of_mon0]),
                filename='monmap',
                )

            # no need to do more than once per host
            break

        # mon.0 is now exporting its data, wait until mon.N has copied it
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in all_ips]
        self.job.barrier(
            hostid=barrier_ids[number],
            tag='mon0_copied',
            ).rendezvous(*barrier_ids)

        if 'mon.0' in my_roles:
            mon0_serve.sp.terminate()
            utils.join_bg_jobs([mon0_serve])

        daemons = []

        if any(r.startswith('mon.') for r in my_roles):
            utils.system('{bindir}/osdmaptool --clobber --createsimple {num_osd} osdmap --pg_bits 2 --pgp_bits 4'.format(
                    num_osd=num_instances_of_type(all_roles, 'osd'),
                    bindir=ceph_bin,
                    conf=self.ceph_conf.filename,
                    ))

            for id_ in roles_of_type(my_roles, 'mon'):
                utils.system('{bindir}/cmon --mkfs -i {id} -c {conf} --monmap=monmap --osdmap=osdmap --keyring=ceph.keyring'.format(
                        bindir=ceph_bin,
                        id=id_,
                        conf=self.ceph_conf.filename,
                        ))
                proc = utils.BgJob(command='{bindir}/cmon -f -i {id} -c {conf}'.format(
                        bindir=ceph_bin,
                        id=id_,
                        conf=self.ceph_conf.filename,
                        ))
                daemons.append(proc)

            os.unlink('monmap')
            os.unlink('osdmap')

        for id_ in roles_of_type(my_roles, 'osd'):
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=osd.{id} dev/osd.{id}.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    ))

        for id_ in roles_of_type(my_roles, 'mds'):
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=mds.{id} dev/mds.{id}.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    ))

        for id_ in roles_of_type(my_roles, 'client'):
            # TODO this --name= is not really obeyed, all unknown "types" are munged to "client"
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=client.{id} client.{id}.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    ))


        # copy keys from osd
        publish = []
        for id_ in roles_of_type(my_roles, 'osd'):
            publish.append('--publish=/key/osd.{id}.keyring:dev/osd.{id}.keyring'.format(id=id_))
        for id_ in roles_of_type(my_roles, 'mds'):
            publish.append('--publish=/key/mds.{id}.keyring:dev/mds.{id}.keyring'.format(id=id_))
        for id_ in roles_of_type(my_roles, 'client'):
            publish.append('--publish=/key/client.{id}.keyring:client.{id}.keyring'.format(id=id_))
        key_serve = utils.BgJob(command='env PYTHONPATH={at_bindir} python -m teuthology.ceph_serve_file --port=11601 {publish}'.format(
                at_bindir=self.bindir,
                publish=' '.join(publish),
                ))

        if 'mon.0' in my_roles:
            for type_, caps in [
                ('osd', '--cap mon "allow *" --cap osd "allow *"'),
                ('mds', '--cap mon "allow *" --cap osd "allow *" --cap mds "allow"'),
                ('client', '--cap mon "allow r" --cap osd "allow rw pool=data" --cap mds "allow"'),
                ]:
                for idx, host_roles in enumerate(all_roles):
                    print 'Fetching {type} keys from host {idx} ({ip})...'.format(
                        type=type_,
                        idx=idx,
                        ip=all_ips[idx],
                        )
                    for id_ in roles_of_type(host_roles, type_):
                        urlretrieve_retry(
                            url='http://{ip}:11601/key/{type}.{id}.keyring'.format(
                                ip=all_ips[idx],
                                type=type_,
                                id=id_,
                                ),
                            filename='temp.keyring',
                            )
                        utils.system('{bindir}/cauthtool temp.keyring --name={type}.{id} {caps}'.format(
                                bindir=ceph_bin,
                                type=type_,
                                id=id_,
                                caps=caps,
                                ))
                        utils.system('{bindir}/ceph -c {conf} -k ceph.keyring -i temp.keyring auth add {type}.{id}'.format(
                                bindir=ceph_bin,
                                conf=self.ceph_conf.filename,
                                type=type_,
                                id=id_,
                                ))

        # TODO where does this belong?
        if 'mon.0' in my_roles:
           utils.system('{bindir}/ceph -c {conf} -k ceph.keyring mds set_max_mds {num_mds}'.format(
                    bindir=ceph_bin,
                    conf=self.ceph_conf.filename,
                    num_mds=num_instances_of_type(all_roles, 'mds'),
                    ))

        # wait until osd/mds/client keys have been copied and authorized
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in all_ips]
        self.job.barrier(
            hostid=barrier_ids[number],
            tag='authorized',
            ).rendezvous(*barrier_ids)
        key_serve.sp.terminate()
        utils.join_bg_jobs([key_serve])

        for id_ in roles_of_type(my_roles, 'osd'):
            os.mkdir(os.path.join('dev', 'osd.{id}.data'.format(id=id_)))
            utils.system('{bindir}/cosd --mkfs -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            proc = utils.BgJob(command='{bindir}/cosd -f -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            daemons.append(proc)

        for id_ in roles_of_type(my_roles, 'mds'):
            proc = utils.BgJob(command='{bindir}/cmds -f -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            daemons.append(proc)

        if 'mon.0' in my_roles:
            # others wait on barrier
            ceph.wait_until_healthy(self)

            utils.system('{bindir}/ceph -c {conf} -s'.format(
                    bindir=ceph_bin,
                    conf=self.ceph_conf.filename,
                    ))

        # server is now healthy
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in all_ips]
        self.job.barrier(
            hostid=barrier_ids[number],
            tag='healthy',
            ).rendezvous(*barrier_ids)

        for id_ in roles_of_type(my_roles, 'client'):
            # TODO support kernel clients too; must use same role due
            # to "type" limitations
            mnt = os.path.join(self.tmpdir, 'mnt.{id}'.format(id=id_))
            os.mkdir(mnt)
            fuse = utils.BgJob(
                # we could use -m instead of ceph.conf, but as we need
                # ceph.conf to find the keyring anyway, it's not yet worth it
                command='{bindir}/cfuse -c {conf} {mnt}'.format(
                    bindir=ceph_bin,
                    conf=self.ceph_conf.filename,
                    mnt=mnt,
                    ),
                stdout_tee=utils.TEE_TO_LOGS,
                stderr_tee=utils.TEE_TO_LOGS,
                )

            try:
                ceph.wait_until_fuse_mounted(self, fuse=fuse, mountpoint=mnt)
                try:
                    aaa = os.path.join(mnt, 'aaa.{id}'.format(id=id_))
                    with file(aaa, 'w') as f:
                        f.write('foo\n')
                    bbb = os.path.join(mnt, 'bbb.{id}'.format(id=id_))
                    os.rename(aaa, bbb)
                    with file(bbb) as f:
                        data = f.read()
                    assert data == 'foo\n'
                finally:
                    utils.system('fusermount -u {mnt}'.format(mnt=mnt))
            finally:
                print 'Waiting for cfuse to exit...'
                utils.join_bg_jobs([fuse])

        # wait until client is done
        barrier_ids = ['{ip}#cluster'.format(ip=ip) for ip in all_ips]
        self.job.barrier(
            hostid=barrier_ids[number],
            tag='done',
            ).rendezvous(*barrier_ids)

        for d in daemons:
            d.sp.terminate()
        utils.join_bg_jobs(daemons)
