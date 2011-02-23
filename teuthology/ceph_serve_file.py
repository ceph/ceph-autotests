import errno
import optparse
import wsgiref.simple_server

class ServeFile(object):

    def __init__(self, urls):
        self.urls = urls

    def serve(self, environ, start_response):
        path = self.urls.get(environ['PATH_INFO'])
        if path is None:
            start_response(
                '404 Not Known',
                [('Content-type', 'text/plain')],
                )
            return ['Not known']

        try:
            f = file(path, 'rb')
        except IOError as e:
            if e.errno == errno.ENOENT:
                start_response(
                    '404 Not Found',
                    [('Content-type', 'text/plain')],
                    )
                return ['Known but not found']
            else:
                start_response(
                    '500 Internal Error',
                    [('Content-type', 'text/plain')],
                    )
                return ['Cannot open: {error}'.format(error=e)]
        else:
            start_response(
                '200 OK',
                [('Content-type', 'application/octet-stream')],
                )
            return f

def main():
    parser = optparse.OptionParser(
        usage='%prog --port=PORT --publish=URLPATH:FILE [OPTS]',
        )
    parser.add_option(
        '--port',
        metavar='PORT',
        type='int',
        )
    parser.add_option(
        '--publish',
        metavar='URL:FILE',
        action='append',
        )
    opts, args = parser.parse_args()
    if args:
        parser.error('Did not expect arguments.')
    if opts.port is None:
        parser.error('Missing option --port.')
    if not opts.publish:
        parser.error('Missing option --publish.')

    urls = {}
    for s in opts.publish:
        try:
            u,f = s.split(':', 1)
        except ValueError:
            parser.error('Invalid format for --publish value: {input}'.format(input=s))
        urls[u] = f

    app = ServeFile(urls=urls)
    httpd = wsgiref.simple_server.make_server('', opts.port, app.serve)
    print "Serving HTTP on port {port}...".format(port=opts.port)

    httpd.serve_forever()

if __name__ == '__main__':
    main()
