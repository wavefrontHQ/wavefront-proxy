#!/usr/bin/env python
"""
This is a test runner with test suite for testing various proxy endpoints.
"""

import datetime
import BaseHTTPServer
import pickle
import socket
import struct
import subprocess
import StringIO
import threading
import tokenize
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
                # use this for debugging if needed
                # print('Adding %s request for |%s|\n%s' %
                #       (command, path, content))
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

class TestAgentProxyBase(object):
    """
    Base class for all test cases that are testing the agent.  This class
    will startup (and shutdown) a fake "metrics.wavefront.com" end point
    and also start the agent itself.
    """

    def __init__(self, *args, **kwargs):
        super(TestAgentProxyBase, self).__init__(*args, **kwargs)
        self.server = None
        self.endpoint_thread = None
        self.agent_stdout_fd = None
        self.agent_process = None

    def setUpCommon(self, test_name):
        """
        Unit Test setup function.  Sets up the fake wavefront endpoint
        and starts the agent.

        Arguments:
        test_name - name of the test being run
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
        self.agent_stdout_fd = open('./test/proxy' + test_name + '.out', 'w')
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

    def tearDownCommon(self):
        self.cleanup()


class TestWriteHttpProxy(unittest.TestCase, TestAgentProxyBase):
    """
    Tests the write_http plugin support (only JSON is supported/tested)
    """

    def __init__(self, *args, **kwargs):
        super(TestWriteHttpProxy, self).__init__(*args, **kwargs)

    def setUp(self):
        self.setUpCommon(self.__class__.__name__)

    def tearDown(self):
        self.tearDownCommon()

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
        # uncomment this and the print conn.read() to see the full request
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


class TestPickleProtocolProxy(unittest.TestCase, TestAgentProxyBase):
    """
    Tests the opentsdb pickle protocol support
    """

    def __init__(self, *args, **kwargs):
        super(TestPickleProtocolProxy, self).__init__(*args, **kwargs)

    def setUp(self):
        self.setUpCommon(self.__class__.__name__)

    def tearDown(self):
        self.tearDownCommon()

    def test_example_capture_1(self):
        """
        Test of a capture sent to us by Rocket Fuel.
        """

        # connect to the proxy on port 5878 - the pickle protocol port
        s = socket.socket()
        s.connect(('127.0.0.1', 5878))

        # open the sample file and send to proxy on open socket
        # the file is graphite pickle format which has a format that looks like:
        # <4 byte unsigned integer length><pickle data>
        # [<4 byte unsigned integer length><pickle data>]...
        # all_pickled list stores the pickle.loads() parsed output from each
        all_pickled = []
        all_data = []
        with open('test/pickle/sample1.pkl', 'rb') as sample_fd:
            # grab 4 byte uint repersenting the length
            orig_length = sample_fd.read(4)
            while orig_length:
                all_data.append(orig_length)
                # the length is big-endian (>) unsigned int (I)
                length = struct.unpack('>I', orig_length)[0]
                # read the length # of bytes from the file now
                data = sample_fd.read(length)
                if data:
                    # all_data stores everything in the file (one record per
                    # list item)
                    all_data.append(data)
                    # all_pickled stores the parsed pickled data
                    # (one list item per pickled segment)
                    all_pickled.append(pickle.loads(data))

                # read the next segment
                orig_length = sample_fd.read(4)

            # send the entire file on the socket using the expected format
            s.sendall(''.join(all_data))

        # all done.  close the socket
        s.shutdown(socket.SHUT_WR)
        s.close()

        # wait for pushdata events to send all data to our fake metrics endpoint
        lines = []
        for i in range(5):
            print 'Waiting for pushdata (' + str(i) + ') event ...'
            self.server.requests.pushdata_event.wait(60)
            self.server.requests.pushdata_event.clear()
            self.assertTrue('pushdata' in self.server.requests.commands)

            # get the contents of the pushdata event and add it to our current
            # list of events
            if i < len(self.server.requests.commands['pushdata']):
                pushdata = self.server.requests.commands['pushdata'][i]
                pushdata_lines = pushdata.splitlines()
                for tmp in pushdata_lines:
                    lines.append(tmp)

            else:
                # assumption: if that last .wait() timed out, then there are
                # no more pushdata events
                break

        current = 0
        for pickled_data in all_pickled:
            # walk through each record in the pickled segment and make sure
            # it matches what the proxy sent to WF.
            # pickle record:
            #   ('metric name', (timestamp, value))
            for metric in pickled_data:
                # split up the metric name into its individual parts
                # [0] -> skipped
                # [1] -> host name
                # [2-n] -> the metric name sent to WF
                parts = metric[0].split('.')
                metric_name = '.'.join(parts[2:])
                # skip over any that have invalid characters in them (will be
                # blocked by proxy)
                if '#' in metric_name:
                    continue
                host_name = parts[1]
                ts = int(metric[1][0]) # remove .0 with int()
                value = metric[1][1]

                # now grab the line from the pushdata content that should match
                line = lines[current]
                # tokenize the line
                tokens = tokenize.generate_tokens(
                    StringIO.StringIO(line).readline)

                # "metric name" value ts source=hostname
                token_number = 0
                for token in tokens:
                    tmp = token[4][token[2][1]:token[3][1]]
                    # strip off the quotes
                    if len(tmp) > 0 and tmp[0] == '"':
                        tmp = tmp[1:-1]

                    # metric name
                    if token_number == 0:
                        self.assertEquals(tmp, metric_name)

                    # value
                    elif token_number == 1:
                        if tmp == '-': # negative numbers in 2 tokens
                            tmp = tmp + tokens.next()[1]
                        self.assertEquals(float(tmp), value)

                    # timestamp
                    elif token_number == 2:
                        self.assertEquals(long(tmp), ts)

                    # host name (skip source, =)
                    elif token_number == 5:
                        self.assertEquals(tmp, host_name)

                    token_number = token_number + 1

                current = current + 1

if __name__ == '__main__':
    unittest.main()
