import os
import pkg_resources
import restish.app
import subprocess
import tarfile

from restish import resource
from restish import http

class Teuthology(resource.Resource):

    @resource.GET()
    def serve(self, request):
        f = pkg_resources.resource_stream(
            'teuthology.html',
            'root.html',
            )
        return http.ok(
            [('Content-Type', 'text/html')],
            f,
            )

    @resource.child('tarball')
    def tarball_bad(self, request, segments):
        return http.forbidden(
            [('Content-Type', 'text/plain')],
            'You need to specify the revision and test.\n',
            )

    # autotest does a test fetch of this url, it has to be 200 ok
    @resource.child('tarball/{rev}')
    def tarball_rev(self, request, segments, **kw):
        return http.ok(
            [('Content-Type', 'text/plain')],
            'You need to also specify the test you want.\n',
            )

    @resource.child('tarball/{rev}/{test}')
    def tarball(self, request, segments, **kw):
        test = kw['test']
        SUFFIX = '.tar.bz2'
        if not test.endswith(SUFFIX):
            return self.tarball_bad(request, segments)
        kw['test'] = test[:-len(SUFFIX)]
        return Tarball(**kw)

class GitArchiveError(Exception):
    pass

class Tarball(resource.Resource):

    def __init__(self, rev, test):
        super(Tarball, self).__init__()
        self.rev = rev
        self.test = test

    def _archive(self, git_dir, rev, path=None, prefix=None):
        args = [
            'git',
            '--git-dir={git_dir}'.format(git_dir=git_dir),
            'archive',
            '--format=tar',
            ]
        if prefix is not None:
            args.append('--prefix={prefix}'.format(prefix=prefix))
        args.extend([
                '--',
                rev,
                ])
        if path is not None:
            args.append(path)
        proc = subprocess.Popen(
            args=args,
            env={},
            close_fds=True,
            stdout=subprocess.PIPE,
            )
        tar = tarfile.open(
            fileobj=proc.stdout,
            mode='r|',
            )
        while True:
            tarinfo = tar.next()
            if tarinfo is None:
                break
            f = tar.extractfile(tarinfo)
            yield (tarinfo, f)
        returncode = proc.wait()
        if returncode != 0:
            raise GitArchiveError(
                'git archive failed with %d: rev=%r path=%r' % (returncode, rev, path))

    @resource.GET()
    def serve(self, request):
        git_dir = request.environ['config']['git-dir']
        tmp = os.tmpfile()
        tar_out = tarfile.open(
            name='/{test}.tar.bz2'.format(test=self.test),
            mode='w|bz2',
            fileobj=tmp,
            )
        try:
            for (tarinfo, fileobj) in self._archive(
                git_dir=git_dir,
                rev='{rev}:tests/{test}/'.format(rev=self.rev, test=self.test),
                ):
                tar_out.addfile(tarinfo, fileobj=fileobj)
        except GitArchiveError as e:
            return http.not_found(
                [('Content-Type', 'text/plain')],
                'Revision or test not found.\n',
                )

        # this one is expected to work; don't catch exceptions
        for (tarinfo, fileobj) in self._archive(
            git_dir=git_dir,
            rev='{rev}'.format(rev=self.rev),
            path='teuthology/'.format(test=self.test),
            ):
            tar_out.addfile(tarinfo, fileobj=fileobj)
        tar_out.close()

        tmp.seek(0)
        res = http.Response(
            status='200 OK',
            headers=[('Content-Type', 'application/x-bzip')],
            body=tmp,
            )
        return res

def setup_environ(app, global_config, local_config):
    def application(environ, start_response):
        environ.setdefault('config', local_config)
        return app(environ, start_response)
    return application

def app_factory(global_config={}, **local_config):
    root = Teuthology()
    app = restish.app.RestishApp(root)
    app = setup_environ(app, global_config, local_config)
    return app
