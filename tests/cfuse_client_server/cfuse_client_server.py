import os
import socket

from autotest_lib.client.bin import test
from autotest_lib.client.bin import utils

from teuthology import ceph

class cfuse_client_server(test.test):
    version = 1

    num_mon = 3
    num_osd = 1
    num_mds = 3

    def setup(self, **kwargs):
        ceph.get_binaries(self, kwargs.get('ceph_bin_url'))

    def run_once(self, **kwargs):
        role = kwargs['role']
        fn = getattr(self, 'run_once_role_{role}'.format(role=role))
        assert fn is not None
        return fn(**kwargs)

    def run_once_role_server(self, **kwargs):
        print 'This is the server...'
        print 'Entering tmp directory:', self.tmpdir
        os.chdir(self.tmpdir)

        # let ceph.conf use fixed pathnames
        os.symlink(self.resultsdir, 'results')
        os.mkdir('results/log')
        os.mkdir('results/profiling-logger')

        os.mkdir('dev')

        self.ceph_bindir = ceph_bin = os.path.join(self.bindir, 'usr/local/bin')

        self.ceph_conf = ceph.skeleton_config(self)
        for id_ in range(self.num_mon):
            section = 'mon.{id}'.format(id=id_)
            self.ceph_conf.setdefault(section, {})
            self.ceph_conf[section]['mon addr'] = '{ip}:{port}'.format(
                ip=kwargs['server_ip'],
                port=6789+id_,
                )
        self.ceph_conf.write()
        print 'Wrote config to', self.ceph_conf.filename

        utils.system('{bindir}/osdmaptool --clobber --createsimple {num_osd} osdmap --pg_bits 2 --pgp_bits 4'.format(
                num_osd=self.num_osd,
                bindir=ceph_bin,
                conf=self.ceph_conf.filename,
                ))

        utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=mon. ceph.keyring'.format(
                bindir=ceph_bin,
                ))

        utils.system('{bindir}/cauthtool --gen-key --name=client.admin --set-uid=0 --cap mon "allow *" --cap osd "allow *" --cap mds "allow" ceph.keyring'.format(
                bindir=ceph_bin,
                ))

        ceph.create_simple_monmap(self)

        daemons = []

        for id_ in range(self.num_mon):
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

        for id_ in range(self.num_osd):
            os.mkdir(os.path.join('dev', 'osd.{id}.data'.format(id=id_)))
            utils.system('{bindir}/cosd --mkfs -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=osd.{id} --cap mon "allow *" --cap osd "allow *" dev/osd.{id}.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    ))
            utils.system('{bindir}/ceph -c {conf} -k ceph.keyring -i dev/osd.{id}.keyring auth add osd.{id}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))

        for id_ in range(self.num_mds):
            utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=mds.{id} --cap mon "allow *" --cap osd "allow *" --cap mds "allow" dev/mds.{id}.keyring'.format(
                    bindir=ceph_bin,
                    id=id_,
                    ))
            utils.system('{bindir}/ceph -c {conf} -k ceph.keyring -i dev/mds.{id}.keyring auth add mds.{id}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))

        utils.system('{bindir}/ceph -c {conf} -k ceph.keyring mds set_max_mds {num_mds}'.format(
                bindir=ceph_bin,
                conf=self.ceph_conf.filename,
                num_mds=self.num_mds,
                ))

        for id_ in range(self.num_osd):
            proc = utils.BgJob(command='{bindir}/cosd -f -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            daemons.append(proc)

        for id_ in range(self.num_mds):
            proc = utils.BgJob(command='{bindir}/cmds -f -i {id} -c {conf}'.format(
                    bindir=ceph_bin,
                    id=id_,
                    conf=self.ceph_conf.filename,
                    ))
            daemons.append(proc)

        ceph.wait_until_healthy(self)

        utils.system('{bindir}/ceph -s -c {conf}'.format(
                bindir=ceph_bin,
                conf=self.ceph_conf.filename,
                ))

        # server is now healthy, wait until client is exporting its key
        server_id = '{ip}#cfuse_client_server-server'.format(ip=kwargs['server_ip'])
        client_id = '{ip}#cfuse_client_server-client'.format(ip=kwargs['client_ip'])
        barrier = self.job.barrier(
            hostid=server_id,
            tag='healthy',
            )
        barrier.rendezvous(
            server_id,
            client_id,
            )

        # copy client key and import it
        sock = socket.socket()
        sock.connect((kwargs['client_ip'], 1234))
        with file('client.keyring', 'w') as f:
            while True:
                data = sock.recv(8192)
                if not data:
                    break
                f.write(data)
        sock.close()

        utils.system('{bindir}/ceph -c {conf} -k ceph.keyring -i client.keyring auth add client.cfuse'.format(
                    bindir=ceph_bin,
                    conf=self.ceph_conf.filename,
                    ))

        # server has added the key
        server_id = '{ip}#cfuse_client_server-server'.format(ip=kwargs['server_ip'])
        client_id = '{ip}#cfuse_client_server-client'.format(ip=kwargs['client_ip'])
        barrier = self.job.barrier(
            hostid=server_id,
            tag='authorized',
            )
        barrier.rendezvous(
            server_id,
            client_id,
            )

        # wait until client is done
        server_id = '{ip}#cfuse_client_server-server'.format(ip=kwargs['server_ip'])
        client_id = '{ip}#cfuse_client_server-client'.format(ip=kwargs['client_ip'])
        self.job.barrier(
            hostid=server_id,
            tag='done',
            ).rendezvous(
            server_id,
            client_id,
            )

        for d in daemons:
            d.sp.terminate()
        utils.join_bg_jobs(daemons)

    def run_once_role_client(self, **kwargs):
        print 'This is the client...'
        print 'Entering tmp directory:', self.tmpdir
        os.chdir(self.tmpdir)

        # let ceph.conf use fixed pathnames
        os.symlink(self.resultsdir, 'results')
        os.mkdir('results/log')
        os.mkdir('results/profiling-logger')

        self.ceph_bindir = ceph_bin = os.path.join(self.bindir, 'usr/local/bin')

        self.ceph_conf = ceph.skeleton_config(self)
        for id_ in range(self.num_mon):
            section = 'mon.{id}'.format(id=id_)
            self.ceph_conf.setdefault(section, {})
            self.ceph_conf[section]['mon addr'] = '{ip}:{port}'.format(
                ip=kwargs['server_ip'],
                port=6789+id_,
                )
        self.ceph_conf.setdefault('cfuse', {})
        self.ceph_conf['cfuse']['keyring'] = 'client.keyring'
        self.ceph_conf.write()

        utils.system('{bindir}/cauthtool --create-keyring --gen-key --name=client.cfuse --cap mon "allow r" --cap osd "allow rw pool=data" --cap mds "allow" client.keyring'.format(
                bindir=ceph_bin,
                ))

        # export key
        sock = socket.socket()
        sock.bind(('0.0.0.0', 1234))
        sock.listen(1)

        # client is now exporting its key, wait until server is healthy
        server_id = '{ip}#cfuse_client_server-server'.format(ip=kwargs['server_ip'])
        client_id = '{ip}#cfuse_client_server-client'.format(ip=kwargs['client_ip'])
        barrier = self.job.barrier(
            hostid=client_id,
            tag='healthy',
            )
        barrier.rendezvous(
            server_id,
            client_id,
            )

        conn, addr = sock.accept()
        print 'Serving key to', addr
        with file('client.keyring') as f:
            while True:
                data = f.read(8192)
                if not data:
                    break
                conn.sendall(data)
        conn.close()
        sock.close()

        # wait until server has imported our key
        server_id = '{ip}#cfuse_client_server-server'.format(ip=kwargs['server_ip'])
        client_id = '{ip}#cfuse_client_server-client'.format(ip=kwargs['client_ip'])
        barrier = self.job.barrier(
            hostid=client_id,
            tag='authorized',
            )
        barrier.rendezvous(
            server_id,
            client_id,
            )

        mnt = os.path.join(self.tmpdir, 'mnt')
        os.mkdir(mnt)
        fuse = utils.BgJob(
            # we could use -m instead of ceph.conf, but as we need
            # ceph.conf to find the keyring anyway, it's not yet worth it
            command='{bindir}/cfuse -c {conf} --name=cfuse {mnt}'.format(
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
                one = os.path.join(mnt, 'one')
                with file(one, 'w') as f:
                    f.write('foo\n')
                two = os.path.join(mnt, 'two')
                os.rename(one, two)
                with file(two) as f:
                    data = f.read()
                assert data == 'foo\n'
            finally:
                utils.system('fusermount -u {mnt}'.format(mnt=mnt))
        finally:
            print 'Waiting for cfuse to exit...'
            utils.join_bg_jobs([fuse])

        # client is now done
        server_id = '{ip}#cfuse_client_server-server'.format(ip=kwargs['server_ip'])
        client_id = '{ip}#cfuse_client_server-client'.format(ip=kwargs['client_ip'])
        self.job.barrier(
            hostid=client_id,
            tag='done',
            ).rendezvous(
            server_id,
            client_id,
            )
