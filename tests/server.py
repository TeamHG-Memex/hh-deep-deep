#!/usr/bin/env python
import argparse
import uuid

from twisted.internet import reactor
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.web.util import Redirect


def text_resource(content):
    class Page(Resource):
        isLeaf = True

        def render_GET(self, request):
            request.setHeader(b'content-type', b'text/html')
            request.setHeader(b'charset', b'utf-8')
            return (content if isinstance(content, bytes) else
                    content.encode('utf8'))
    return Page()


def get_session_id(request):
    return request.received_cookies.get(b'_test_auth')


def is_authenticated(request):
    session_id = get_session_id(request)
    if session_id not in SESSIONS:
        return False

    if SESSIONS[session_id]:
        return True
    else:
        request.setHeader(b'set-cookie', b'_test_auth=')
        return False


def authenticated_text(content):
    class R(Resource):
        def render_GET(self, request):
            if not is_authenticated(request):
                return Redirect(b'/login').render(request)
            else:
                return content.encode()
    return R()


SESSIONS = {}  # session_id -> logged_in?


def login(request):
    session_id = uuid.uuid4().hex.encode('ascii')
    SESSIONS[session_id] = True
    request.setHeader(b'set-cookie', b'_test_auth=' + session_id)


def logout(request):
    session_id = get_session_id(request)
    if session_id is not None:
        SESSIONS[session_id] = False
    request.setHeader(b'set-cookie', b'_test_auth=')


class LoginSite(Resource):
    class _Login(Resource):
        isLeaf = True

        def render_GET(self, request):
            if is_authenticated(request):
                return Redirect(b'/').render(request)
            return (
                b'<form action="/login" method="POST">'
                b'<input type="text" name="login">'
                b'<input type="password" name="password">'
                b'<input type="submit" value="Login">'
                b'</form>')

        def render_POST(self, request):
            if request.args[b'login'][0] == b'admin' and \
                            request.args[b'password'][0] == b'secret':
                login(request)
            return Redirect(b'/').render(request)

    class _Index(Resource):
        isLeaf = True

        def render_GET(self, request):
            if is_authenticated(request):
                return (
                    b'<a href="/hidden">hidden</a> '
                    b'<a href="/hidden-2">hidden-2</a> '
                )
            else:
                return b'<a href="/login">Login</a><a href="/open">open</a>'

    def __init__(self):
        Resource.__init__(self)
        self.putChild(b'', self._Index())
        self.putChild(b'open', text_resource('<a href="/more">more</a>'))
        self.putChild(b'more', text_resource('no more'))
        self.putChild(b'login', self._Login())
        self.putChild(b'hidden', authenticated_text('hidden resource'))
        self.putChild(b'hidden-2', authenticated_text('hidden resource 2'))


PORT = 8781


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('resource', choices=['login'])
    args = parser.parse_args()
    resource = {'login': LoginSite}[args.resource]()
    http_port = reactor.listenTCP(PORT, Site(resource))

    def print_listening():
        host = http_port.getHost()
        print('Mock server {} running at http://{}:{}'.format(
            resource, host.host, host.port))

    reactor.callWhenRunning(print_listening)
    reactor.run()


if __name__ == '__main__':
    main()
