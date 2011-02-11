from nose.tools import eq_ as eq
import tarfile
import webtest

from cStringIO import StringIO

from teuthology import web
from teuthology.test import util

def test_root():
    app = web.app_factory()
    app = webtest.TestApp(app)
    res = app.get('/')
    eq(res.headers['Content-Type'], 'text/html')
    eq(''.join(res.lxml.xpath('//head/title/text()')), 'Teuthology -- Ceph Autotest infrastructure')
    eq(''.join(res.lxml.xpath('//body/h1/text()')), 'You need to specify what you want')

def test_tarball():
    app = web.app_factory()
    app = webtest.TestApp(app)
    res = app.get('/tarball', status=403)
    eq(res.headers['Content-Type'], 'text/plain')
    eq(res.body, 'You need to specify the revision and test.\n')

def test_tarball_slash():
    app = web.app_factory()
    app = webtest.TestApp(app)
    res = app.get('/tarball', status=403)
    eq(res.headers['Content-Type'], 'text/plain')
    eq(res.body, 'You need to specify the revision and test.\n')

def test_tarball_master():
    # autotest requires this to serve something valid
    app = web.app_factory()
    app = webtest.TestApp(app)
    res = app.get('/tarball/master')
    eq(res.headers['Content-Type'], 'text/plain')
    eq(res.body, 'You need to also specify the test you want.\n')

def test_tarball_master_slash():
    app = web.app_factory()
    app = webtest.TestApp(app)
    res = app.get('/tarball/master/')
    eq(res.headers['Content-Type'], 'text/plain')
    eq(res.body, 'You need to also specify the test you want.\n')

def test_tarball_simple():
    tmp = util.maketemp()
    util.fake_git(tmp)
    util.fast_import(
        repo=tmp,
        commits=[
            dict(
                message='one',
                committer='John Doe <jdoe@example.com>',
                commit_time='1216235872 +0300',
                files=[
                    dict(
                        path='tests/fake/fake.py',
                        content='faketest',
                        ),
                    dict(
                        path='tests/fake/extra-stuff',
                        content='EXTRA',
                        mode='100755',
                        ),
                    dict(
                        path='teuthology/library-stuff-here',
                        content='fakelib',
                        ),
                    ],
                ),
            ],
        )
    app = web.app_factory(**{'git-dir': tmp})
    app = webtest.TestApp(app)
    res = app.get('/tarball/master/fake.tar.bz2')
    eq(res.headers['Content-Type'], 'application/x-bzip')
    f = StringIO(res.body)
    tar = tarfile.open(
        fileobj=f,
        mode='r:bz2',
        )

    ti = tar.next()
    assert ti is not None
    eq(ti.name, 'fake')
    eq(ti.type, tarfile.DIRTYPE)
    eq('%o'%ti.mode, '775')

    ti = tar.next()
    assert ti is not None
    eq(ti.name, 'fake/extra-stuff')
    eq(ti.type, tarfile.REGTYPE)
    eq('%o'%ti.mode, '775')
    eq(tar.extractfile(ti).read(), 'EXTRA')

    ti = tar.next()
    assert ti is not None
    eq(ti.name, 'fake/fake.py')
    eq(ti.type, tarfile.REGTYPE)
    eq('%o'%ti.mode, '664')
    eq(tar.extractfile(ti).read(), 'faketest')

    # this gets repeated because we're concatenating archives; it has
    # no side effects
    ti = tar.next()
    assert ti is not None
    eq(ti.name, 'fake')
    eq(ti.type, tarfile.DIRTYPE)
    eq('%o'%ti.mode, '775')

    ti = tar.next()
    assert ti is not None
    eq(ti.name, 'fake/teuthology')
    eq(ti.type, tarfile.DIRTYPE)
    eq('%o'%ti.mode, '775')

    ti = tar.next()
    assert ti is not None
    eq(ti.name, 'fake/teuthology/library-stuff-here')
    eq(ti.type, tarfile.REGTYPE)
    eq('%o'%ti.mode, '664')
    eq(tar.extractfile(ti).read(), 'fakelib')

    ti = tar.next()
    assert ti is None

    # TODO validate

def test_tarball_revision_not_found():
    tmp = util.maketemp()
    util.fake_git(tmp)
    util.fast_import(
        repo=tmp,
        commits=[
            dict(
                message='one',
                committer='John Doe <jdoe@example.com>',
                commit_time='1216235872 +0300',
                files=[
                    dict(
                        path='tests/fake/fake.py',
                        content='fakecontrol',
                        ),
                    dict(
                        path='tests/fake/extra-stuff',
                        content='EXTRA',
                        mode='100755',
                        ),
                    dict(
                        path='teuthology/library-stuff-here',
                        content='fakelib',
                        ),
                    ],
                ),
            ],
        )
    app = web.app_factory(**{'git-dir': tmp})
    app = webtest.TestApp(app)
    res = app.get('/tarball/nonexistent/fake.tar.bz2', status=404)
    eq(res.headers['Content-Type'], 'text/plain')
    eq(res.body, 'Revision or test not found.\n')

def test_tarball_test_not_found():
    tmp = util.maketemp()
    util.fake_git(tmp)
    util.fast_import(
        repo=tmp,
        commits=[
            dict(
                message='one',
                committer='John Doe <jdoe@example.com>',
                commit_time='1216235872 +0300',
                files=[
                    dict(
                        path='tests/fake/fake.py',
                        content='fakecontrol',
                        ),
                    dict(
                        path='tests/fake/extra-stuff',
                        content='EXTRA',
                        mode='100755',
                        ),
                    dict(
                        path='teuthology/library-stuff-here',
                        content='fakelib',
                        ),
                    ],
                ),
            ],
        )
    app = web.app_factory(**{'git-dir': tmp})
    app = webtest.TestApp(app)
    res = app.get('/tarball/master/nonexistent.tar.bz2', status=404)
    eq(res.headers['Content-Type'], 'text/plain')
    eq(res.body, 'Revision or test not found.\n')
