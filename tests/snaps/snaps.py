import os

from autotest_lib.client.bin import utils

from teuthology import ceph
from teuthology import skeleton
import re
import subprocess

from time import time,sleep

class snaps(skeleton.CephTest):
    version = 1

    def generate_caps(self, role, id_):
        if role == 'client' and self.client_is_type(id_, 'rados'):
            return '--cap mon "allow rw" --cap osd "allow rwx pool=data,rbd,casdata" --cap mds "allow"'
        return super(snaps,self).generate_caps(role,id_)

    @skeleton.role('client')
    def do_102_run_work(self):
        self.results = {}
        procs = {};
        for id_ in skeleton.roles_of_type(self.my_roles, 'client'):
            procs[id_] = subprocess.Popen(
                [
                    '{bindir}/usr/local/bin/testsnaps'.format(
                        bindir=self.bindir
                        ),
                    ],
                env = {
                    'LD_LIBRARY_PATH': '{bindir}/usr/local/lib'.format(
                        bindir=self.bindir
                        ),
                    'CEPH_CLIENT_ID': id_
                    },
                stderr = subprocess.STDOUT,
                stdout = subprocess.PIPE
                )
                
            print "Starting run clinet {id_}".format(id_=id_)

        for id_ in procs:
            procs[id_].wait()
            print "client.{id_} finished with errorcode {code}".format(
                id_=id_,
                code=procs[id_].returncode,
                )
            output = procs[id_].communicate()[0]
            self.results['client.{id}'.format(id=id_)] = output
            print output

    def postprocess(self):
        print "postprocess"
        result = {}
        for client in self.results:
            output = self.results[client]
            lines = filter(
                lambda x: re.match(
                    r'.* latency: $|.*percentile.*ms$', x
                    ), 
                output.splitlines()
                )
            lines = [x.lstrip() for x in lines]
            cur = lines.pop(0)
            prefix = re.match(r'(.*) latency: $', cur).group(1)
            while lines:
                cur = lines.pop(0)
                if 'latency:' in cur:
                    prefix = re.match(r'^(.*) latency: $', cur).group(1)
                else:
                    key = prefix + "_" + re.match(r'^(\d+)th percentile.*$', cur).group(1) + "_percentile"
                    result[key] = int(re.match(r'^.*\D(\d+)ms$', cur).group(1))
            self.write_perf_keyval(result)
