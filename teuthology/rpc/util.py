def readlines(sock):
    buf = ''
    while True:
        more = sock.recv(8192)
        if not more:
            if buf:
                raise RuntimeError("Partial last line: %r", buf)
            break
        buf += more
        while True:
            try:
                line, buf = buf.split('\n', 1)
            except ValueError:
                break
            yield line

        if len(buf) > 10000:
            raise RuntimeError('Line too long.')
