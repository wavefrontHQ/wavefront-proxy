#!/usr/bin/env python
"""
This is a test runner with test suite for testing various proxy endpoints.
"""

import datetime
import BaseHTTPServer
import subprocess
import threading
import unittest
import urllib2

import dateutil
import dateutil.tz

EPOCH = (datetime.datetime.utcfromtimestamp(0)
         .replace(tzinfo=dateutil.tz.tzutc()))
def unix_time_seconds(date_in):
    """
    Convert a datetime into unix epoch seconds
    Arguments:
    date_in - the datetime object to convert. This must have a tz = UTC
    """
    return (date_in - EPOCH).total_seconds()


class HttpServerRequests(object):
    """
    HTTP Requests object is used to store requests that have been sent to
    the HTTP server.
    """

    def __init__(self):
        """
        Construct this class
        """

        self.lock = threading.Lock()
        self.event = threading.Event()
        self.config_processed_event = threading.Event()
        self.pushdata_event = threading.Event()
        self.commands = {}

    def add_request(self, request_handler):
        """
        Add an incoming request

        Arguments:
        request_handler - the http request handler
        """

        self.lock.acquire()
        try:
            path = request_handler.path
            headers = request_handler.headers
            qsl = path.find('?')
            if qsl >= 0:
                path = path[0:qsl]
            parts = path.split('/')
            if len(parts) > 4:
                length = int(headers.getheader('content-length'))
                command = parts[4].lower()
                if length > 0:
                    content = request_handler.rfile.read(length)
                else:
                    content = ''
                if command not in self.commands:
                    self.commands[command] = [content]
                else:
                    self.commands[command].append(content)
                #print('Adding %s request for |%s|\n%s' %
                #      (command, path, content))
                if command == 'config' and parts[5] == 'processed':
                    self.config_processed_event.set()
                elif command == 'pushdata':
                    self.pushdata_event.set()

            self.event.set()

        finally:
            self.lock.release()
            self.config_processed_event.clear()
            self.pushdata_event.clear()
            self.event.clear()

class AgentHttpEndpointHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    The RequestHandler class that handles requests from the agent (i.e., serves
    as our fake HTTP WF endpoint).
    """

    def do_POST(self):
        """
        Handles the POST request.
        """

        self.server.requests.add_request(self)
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write("{}")

class TestWriteHttpProxy(unittest.TestCase):
    """
    Tests the write_http plugin support (only JSON is supported/tested)
    """

    def setUp(self):
        """
        Unit Test setup function.  Sets up the fake wavefront endpoint
        and connects to the proxy.
        """

        # cleanup function set in case setUp() fails
        self.addCleanup(self.cleanup)

        # create our fake metrics listener in a separate thread
        self.server = BaseHTTPServer.HTTPServer(('127.0.0.1', 8080),
                                                AgentHttpEndpointHandler)
        self.server.requests = HttpServerRequests()
        self.endpoint_thread = threading.Thread(
            target=self.server.serve_forever)
        self.endpoint_thread.start()

        # start the agent
        self.agent_stdout_fd = open('./test/proxy.out', 'w')
        args = ['java', '-jar', '../proxy/target/wavefront-push-agent.jar',
                '-f', './wavefront.conf', '--purgeBuffer']
        self.agent_process = subprocess.Popen(args, stdout=self.agent_stdout_fd,
                                              stderr=self.agent_stdout_fd,
                                              cwd='./test')
        print 'Waiting for /config/processed request ...'
        self.server.requests.config_processed_event.wait(60)

    def cleanup(self):
        """
        Unit Test cleanup function (called even if setUp() fails unlike
        the tearDown()
        """

        if self.agent_process:
            self.agent_process.kill()
            self.agent_process.wait()
            self.agent_process = None

        if self.agent_stdout_fd:
            self.agent_stdout_fd.close()
            self.agent_stdout_fd = None

        if self.server:
            self.server.shutdown()
            self.server = None

        if self.endpoint_thread:
            self.endpoint_thread.join()
            self.endpoint_thread = None

    def tearDown(self):
        self.cleanup()

    def test_simple_1(self):
        """
        Test that a simple JSON file is parsed and sent to WF correctly.
        """

        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        json = """
[
   {
     "values": [197141504, 175136768],
     "dstypes": ["counter", "counter"],
     "dsnames": ["read", "write"],
     "time": %d,
     "interval": 10,
     "host": "simple1.example.org",
     "plugin": "disk",
     "plugin_instance": "sda",
     "type": "disk_octets",
     "type_instance": ""
   }
]
""" % utcnow

        # this is the contents we expect to see in the POST to the WF servers
        pushdata_expect = ("\"disk.sda.disk_octets.read\" 197141504 %d "
                           "source=\"simple1.example.org\"\n"
                           "\"disk.sda.disk_octets.write\" 175136768 %d "
                           "source=\"simple1.example.org\"" % (utcnow, utcnow))

        headers = {
            'Content-Type': 'application/json'
        }
        #handler = urllib2.HTTPHandler(debuglevel=1)
        #opener = urllib2.build_opener(handler)
        #urllib2.install_opener(opener)
        request = urllib2.Request('http://127.0.0.1:4878/',
                                  headers=headers,
                                  data=json)
        conn = urllib2.urlopen(request)
        #print conn.read()

        self.server.requests.pushdata_event.wait(60)
        self.assertEquals(pushdata_expect,
                          self.server.requests.commands['pushdata'][0])

if __name__ == '__main__':
    unittest.main()
