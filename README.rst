================
 Ceph autotests
================

The Ceph project needs automated tests. Because Ceph is a highly
distributed system, and has active kernel development, its testing
requirements are quite different from e.g. typical LAMP web
applications. The best of breed free software test framework that
covers our needs seems to be Autotest.

However, Autotest was created for a very specific use, and while it is
being made more flexible, it still has a lot of assumptions.  To fit
Ceph needs into Autotest thinking, we've created a bit of a special
setup:

.. figure:: overview.png

   .. if you're reading the raw file, look at the file overview.png manually

   .. or update it with
   .. dia -e overview.png -t cairo-alpha-png overview.dia

   Ceph's autotests are stored in ``ceph-autotests.git``. They are
   served as on-demand generated ``.tar.bz2`` by a web service called
   ``teuthology.web``. This is called "external tests" by
   Autotest.

   To make tests run faster, they work by using pre-compiled binaries;
   these are provided either by gitbuilder (using the same web server
   as the status web page), or developers wishing to test their own
   changes (any HTTP URL will work).

   ``control`` files are short Python snippets that tell Autotest how
   to perform a particular test run. They can be entered in a web form,
   or passed to a command-line client ``atest``.


Running the tests
=================

TODO


Adding a test
=============

TODO


Teuthology, a library for common test tasks
===========================================

Most Ceph autotests are expected to perform fairly similar
setup/teardown tasks. These are abstracted into the ``teuthology``
Python library, which is bundled in the test ``.tar.bz2`` by
``teuthology.web``.

Documentation for the library is in its source, as Python
docstrings. See the subdirectory ``teuthology`` for more.


Using worker machines manually
==============================

You can use the autotest worker machines for manual testing, by
*locking* them in the web user interface, or on the command line with
``atest host mod --lock``. Remember to unlock them when done.
