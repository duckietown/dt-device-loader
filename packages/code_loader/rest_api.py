import json
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = 8081
INCLUDE_OUTPUT = False


class CodeLoaderRESTAPI(Thread):

    def __init__(self, code_loader):
        Thread.__init__(self)
        self.code_loader = code_loader
        self.httpd = None

    def run(self):
        server_address = ('', PORT)
        self.httpd = CodeLoaderHTTPServer(server_address, self.code_loader)
        self.httpd.serve_forever()

    def stop(self):
        try:
            self.httpd.shutdown()
            self.httpd.socket.shutdown()
        except:
            pass


class CodeLoaderHTTPRequestHandler(BaseHTTPRequestHandler):

    def _set_headers(self):
        # open headers
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        # support CORS
        if 'Origin' in self.headers:
            self.send_header('Access-Control-Allow-Origin', self.headers['Origin'])
        # close headers
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        code_loader_status = self.server.code_loader.get_status()
        if not INCLUDE_OUTPUT:
            for lvl in code_loader_status['progress']:
                code_loader_status['progress'][lvl]['output'] = None
        res = json.dumps(code_loader_status, indent=4, sort_keys=True).encode()
        self.wfile.write(res)

    def do_HEAD(self):
        self._set_headers()

    def log_message(self, format, *args):
        return


class CodeLoaderHTTPServer(HTTPServer):

    def __init__(self, server_address, code_loader):
        HTTPServer.__init__(self, server_address, CodeLoaderHTTPRequestHandler)
        self.code_loader = code_loader
