#!/usr/bin/env python
"""
This is a test runner with test suite for testing various proxy endpoints.
"""

import datetime
import BaseHTTPServer
import pickle
import re
import socket
import struct
import subprocess
import threading
import time
import unittest
import urllib2
import Queue
import dateutil
import dateutil.tz

import requests

EPOCH = (datetime.datetime.utcfromtimestamp(0)
         .replace(tzinfo=dateutil.tz.tzutc()))
def unix_time_seconds(date_in):
    """
    Convert a datetime into unix epoch seconds
    Arguments:
    date_in - the datetime object to convert. This must have a tz = UTC
    """
    return (date_in - EPOCH).total_seconds()

#pylint: disable=too-few-public-methods
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
        self.events = {
            'main': threading.Event(),
            'pushdata': threading.Event(),
            'config_processed': threading.Event()
        }
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
                    self.commands[command] = Queue.Queue()
                self.commands[command].put(content)

                # use this for debugging if needed
                # print('Adding %s request for |%s|\n%s' %
                #       (command, path, content))
                if command == 'config' and parts[5] == 'processed':
                    self.events['config_processed'].set()

                elif command == 'pushdata':
                    self.events['pushdata'].set()
                    print '%s body:\n|%s|\n' % (command, content)

            self.events['main'].set()

        finally:
            self.lock.release()
            for command, event in self.events.iteritems():
                event.clear()

class AgentHttpEndpointHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    The RequestHandler class that handles requests from the agent (i.e., serves
    as our fake HTTP WF endpoint).
    """

    #pylint: disable=invalid-name
    def do_POST(self):
        """
        Handles the POST request.
        """

        self.server.requests.add_request(self)
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write("{}")

#pylint: disable=too-many-public-methods
class TestAgentProxyBase(unittest.TestCase):
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
        self.current_test_name = None

    def setup_common(self, test_name, port, additional_args=None,
                     conf_file=None):
        """
        Unit Test setup function.  Sets up the fake wavefront endpoint
        and starts the agent.

        Arguments:
        test_name - name of the test being run
        port - port being tested (will wait for it to be up)
        additional_args - additional proxy arguments (should be a list)
        conf_file - configuration file (if different than './wavefront.conf')
        """

        self.current_test_name = test_name
        print ('\n----------------------------\n%s'
               '\n----------------------------') % (test_name, )

        # cleanup function set in case setUp() fails
        self.addCleanup(self.cleanup)

        # create our fake metrics listener in a separate thread
        self.server = BaseHTTPServer.HTTPServer(('127.0.0.1', 8080),
                                                AgentHttpEndpointHandler)
        self.server.requests = HttpServerRequests()
        self.endpoint_thread = threading.Thread(
            target=self.server.serve_forever)
        self.endpoint_thread.start()

        if not conf_file:
            conf_file = './wavefront.conf'

        # start the agent
        print 'Starting proxy [%s] ...' % (test_name)
        self.agent_stdout_fd = open('./test/proxy' + test_name + '.out', 'w')
        args = ['java', '-jar',
                '-Dcom.sun.management.jmxremote.port=3000',
                '-Dcom.sun.management.jmxremote',
                '-Dcom.sun.management.jmxremote.local.only=false',
                '-Dcom.sun.management.jmxremote.authenticate=false',
                '-Dcom.sun.management.jmxremote.ssl=false',
                '-Djava.rmi.server.hostname=192.168.56.101',
                '../proxy/target/wavefront-push-agent.jar',
                '-f', conf_file, '--purgeBuffer'
               ]
        if additional_args:
            args.extend(additional_args)
        self.agent_process = subprocess.Popen(args, stdout=self.agent_stdout_fd,
                                              stderr=self.agent_stdout_fd,
                                              cwd='./test')
        print 'Waiting for /config/processed request ...'
        self.server.requests.events['config_processed'].wait(60)
        self.wait_for_port(port, 60)

    def wait_for_server_command(self, command, timeout):
        """
        Waits for the given command to arrive
        """

        if (command not in self.server.requests.commands or
                self.server.requests.commands[command].empty()):
            print 'Waiting for %s' % (command, )
            self.server.requests.events[command].wait(timeout)

        if command in self.server.requests.commands:
            return self.server.requests.commands[command]
        else:
            print '%s not in commands list' % (command)
            return Queue.Queue()

    def cleanup(self):
        """
        Unit Test cleanup function (called even if setUp() fails unlike
        the tearDown()
        """

        if self.agent_process:
            print 'Killing proxy ...'
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

    def teardown_common(self):
        """
        Common teardown code
        """

        self.cleanup()

    @staticmethod
    def is_port_available(port):
        """
        Checks to see if the given port on localhost is listening.
        Arguments:
        port - the port to check
        """

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', port))
        if result == 0:
            sock.shutdown(socket.SHUT_WR)
            sock.close()
        return result == 0

    def wait_for_port(self, port, timeout):
        """
        Waits for the given amount of time for the port to be listening
        Arguments:
        port - the port to check
        timeout - the maximum number of seconds to wait
        """

        timeleft = timeout
        sleep_time = 1
        while timeleft and not self.is_port_available(port):
            time.sleep(sleep_time)
            timeleft = timeleft - sleep_time

        self.assertTrue(self.is_port_available(port))

    @staticmethod
    def send_messages_and_close(port, message):
        """
        Sends the cmd(s) to the WF proxy on the given port and close the
        socket when done.  Will not close until all messages have been sent.
        Arguments:
        port - the port to connect to
        message - the message to send (string or list or strings)
        """

        # connect to the proxy on port
        sock = socket.socket()
        sock.connect(('127.0.0.1', port))

        # send
        if isinstance(message, list):
            for msg in message:
                sock.sendall(msg)
        else:
            sock.sendall(message)

        # all done.  close the socket
        sock.shutdown(socket.SHUT_WR)
        sock.close()

    @staticmethod
    def send_message_and_close(port, message):
        """
        Sends message to the given port (assumes localhost) and closes the
        socket once message has been sent.
        Arguments:
        port - the port to send the message to (localhost assumed)
        message - the message to send
        """

        sock = socket.socket()
        sock.connect(('127.0.0.1', port))
        sock.sendall(message)
        sock.shutdown(socket.SHUT_WR)
        sock.close()

    @staticmethod
    def send_message_parallel(port, cmds):
        """
        Sends the messages to the WF proxy in parallel (each message in its
        own thread)
        Arguments:
        port - the port on the localhost host to send message to
        cmds - the list of messages to send
        """

        threads = []
        for cmd in cmds:
            thread = threading.Thread(
                target=TestAgentProxyBase.send_message_and_close,
                args=(port, cmd))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
        print '%d threads complete' % (len(cmds), )

    def wait_for_push_data(self, pushdata_expect):
        """
        Wait for the proxy to send all of the expected metric lines.  This will
        wait up to 5 minutes
        Arguments:
        pushdata_expect - an array of expected push data lines (not including
                      the end of line character)
        """

        now = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        start = now
        while len(pushdata_expect) > 0 and (now - start) < 120:
            now = unix_time_seconds(datetime.datetime.utcnow().replace(
                tzinfo=dateutil.tz.tzutc()))
            pushdata = self.wait_for_server_command('pushdata', 60)
            if pushdata.empty():
                print 'pushdata queue is empty'
                continue

            # there could be multiple metric lines in the body
            data = pushdata.get().split('\n')
            for line in data:
                foundline = line in pushdata_expect
                if foundline:
                    #print 'Found |%s|' % (line, )
                    pushdata_expect.remove(line)

                else:
                    self.fail('Line |%s| NOT expected' % (line))

        self.assertEquals(0, len(pushdata_expect))

    def assert_blocked_point_in_log(self, message, replace_special):
        """
        Asserts that this test's log file has a blocked point in with
        at least part of the given message in it.
        Arguments:
        message - the message to look for
        replace_special - true to replace RE special characters in message
        """

        message_re = message
        if replace_special:
            re_special_chars = ['^', '.', '$', '*', ')', '(']
            for char in re_special_chars:
                message_re = message_re.replace(char, '\\' + char)

        logfile_path = './test/proxy' + self.current_test_name + '.out'
        with open(logfile_path, 'r') as proxylog_fd:
            print ('Checking |%s| has blocked message |%s|' %
                   (logfile_path, message_re))
            found = False
            iterations = 0
            while not found and iterations < 6:
                for line in proxylog_fd:
                    if re.match('.*blocked input:.*' + message_re + '.*', line):
                        found = True
                        break
                iterations = iterations + 1
                if not found and iterations < 6:
                    time.sleep(10)

            if not found:
                self.fail('Blocked line NOT found')

#pylint: disable=too-many-public-methods
class TestWriteHttpProxy(TestAgentProxyBase):
    """
    Tests the write_http plugin support (only JSON is supported/tested)
    """

    def __init__(self, *args, **kwargs):
        super(TestWriteHttpProxy, self).__init__(*args, **kwargs)

    def setUp(self):
        self.setup_common(self.__class__.__name__ + '__' +
                          self._testMethodName, 4878)

    def tearDown(self):
        self.teardown_common()

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
        pushdata_expect = ["\"disk.sda.disk_octets.read\" 197141504 %d "
                           "source=\"simple1.example.org\"" % (utcnow),
                           "\"disk.sda.disk_octets.write\" 175136768 %d "
                           "source=\"simple1.example.org\"" % (utcnow)]

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
        #conn =
        urllib2.urlopen(request)
        #print conn.read()

        self.wait_for_push_data(pushdata_expect)

class TestOpenTSDBProtocol(TestAgentProxyBase):
    """
    Tests the opentsdb telnet & http protocol support
    """

    def __init__(self, *args, **kwargs):
        super(TestOpenTSDBProtocol, self).__init__(*args, **kwargs)

    def setUp(self):
        if self._testMethodName[0:12] == 'test_prefix_':
            args = ['-p', self._testMethodName]
        elif self._testMethodName == 'test_metric_invalid_characters':
            args = ['--pushBlockedSamples', '0']
        else:
            args = None
        self.setup_common(self.__class__.__name__ + '__' + self._testMethodName,
                          4242, args)

    def tearDown(self):
        self.teardown_common()

    def test_telnet_1(self):
        """
        Test of a single PUT command
        """

        # send PUT
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = ('put test.test1.t1 %d 0.14 host=localhost\n' % (utcnow,))
        self.send_message_and_close(4242, putcmd)

        # check that it was sent on to the WF server
        pushdata_expect = [("\"test.test1.t1\" 0.14 %d source=\"localhost\"" %
                            (utcnow))]
        self.wait_for_push_data(pushdata_expect)

    def test_telnet_2(self):
        """
        Test of a multiple PUT commands
        """

        # send PUTs
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = [('put test.test1.t1 %d 0.14 host=localhost\n' % (utcnow,)),
                  ('put test.test1.t2 %d 0.15 host=localhost\n' %
                   (utcnow + 1000,))]
        self.send_messages_and_close(4242, putcmd)

        # check that it was sent on to the WF server
        pushdata_expect = [("\"test.test1.t1\" 0.14 %d source=\"localhost\"" %
                            (utcnow)),
                           ("\"test.test1.t2\" 0.15 %d source=\"localhost\"" %
                            (utcnow + 1000))]
        self.wait_for_push_data(pushdata_expect)

    def test_telnet_parallel_1(self):
        """
        Test of a multiple PUT commands in parallel
        """

        # send PUTs
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = [('put test.test1.t1 %d 0.14 host=localhost\n' % (utcnow,)),
                  ('put test.test1.t2 %d 0.15 host=localhost\n' %
                   (utcnow + 1000,))]
        self.send_message_parallel(4242, putcmd)

        # check that it was sent on to the WF server
        pushdata_expect = [("\"test.test1.t1\" 0.14 %d source=\"localhost\"" %
                            (utcnow)),
                           ("\"test.test1.t2\" 0.15 %d source=\"localhost\"" %
                            (utcnow + 1000))]
        self.wait_for_push_data(pushdata_expect)

    def test_telnet_parallel_2(self):
        """
        Test of a multiple PUT commands in parallel
        """

        # send PUTs
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))

        expected_putcmds = []
        for j in range(100):
            putcmds = []
            for i in range(1000):
                putcmds.append('put test.test1.%d.t%d %d %d.0 '
                               'host=localhost\n' % (j, i, utcnow, i))
                expected_putcmds.append('"test.test1.%d.t%d" %d.0 %d '
                                        'source="localhost"' %
                                        (j, i, i, utcnow))
            # add on a version command every 1000
            putcmds.append('version\n')

            self.send_message_parallel(4242, putcmds)
        self.wait_for_push_data(expected_putcmds)

    def test_telnet_version(self):
        """
        Tests that version command returns something via telnet
        """

        sock = socket.socket()
        sock.settimeout(5.0)
        sock.connect(('127.0.0.1', 4242))

        # send
        print 'sending version command'
        sock.sendall('version\n')

        # recv
        print 'waiting for response ...'
        data = sock.recv(4096)
        self.assertEquals('Wavefront OpenTSDB Endpoint\n', data)

        # all done.  close the socket
        sock.shutdown(socket.SHUT_WR)
        sock.close()

    def test_http_with_array(self):
        """
        Test of a single HTTP command with an array of data points
        """

        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc())) * 1000
        body = """[
    {
        "metric": "sys.cpu.nice",
        "timestamp": %d,
        "value": 18,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    },
    {
        "metric": "sys.cpu.nice",
        "timestamp": %d,
        "value": 9,
        "tags": {
           "host": "web02",
           "dc": "lga"
        }
    }
]""" % (utcnow - 3000, utcnow)

        headers = {'content-type': 'application/json'}
        requests.post('http://localhost:4242/api/put',
                      data=body,
                      headers=headers)

        pushdata_expect = ['"sys.cpu.nice" 18 %d source="web01"'
                           ' "dc"="lga"' % ((utcnow - 3000) / 1000),
                           '"sys.cpu.nice" 9 %d source="web02"'
                           ' "dc"="lga"' % (utcnow / 1000)]
        self.wait_for_push_data(pushdata_expect)

    def test_http_with_single_element(self):
        """
        Test of a single HTTP command with only a single data point
        """

        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc())) * 1000
        body = """
    {
        "metric": "sys.cpu.nice",
        "timestamp": %d,
        "value": 18,
        "tags": {
           "host": "web01",
           "dc": "lga"
        }
    }""" % (utcnow)

        headers = {'content-type': 'application/json'}
        requests.post('http://localhost:4242/api/put',
                      data=body, headers=headers)

        pushdata_expect = [('"sys.cpu.nice" 18 %d source="web01"'
                            ' "dc"="lga"') % (utcnow / 1000)]
        self.wait_for_push_data(pushdata_expect)

    def test_http_with_version(self):
        """
        Test HTTP /api/version endpoint
        """

        request = requests.post('http://localhost:4242/api/version')
        self.assertEquals("Wavefront OpenTSDB Endpoint", request.text)

    def test_http_unsupported_endpoint(self):
        """
        Test HTTP /api/blah endpoint
        """

        request = requests.post('http://localhost:4242/api/blah')
        self.assertEquals(400, request.status_code)

    def test_prefix_1(self):
        """
        Test of a single PUT command with a configured prefix
        """

        # send PUT
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = ('put test.test1.t1 %d 0.14 host=localhost\n' % (utcnow,))
        self.send_message_and_close(4242, putcmd)

        # check that it was sent on to the WF server
        pushdata_expect = [('"test_prefix_1.test.test1.t1" 0.14 %d '
                            'source="localhost"' % (utcnow))]
        self.wait_for_push_data(pushdata_expect)

    def test_host_name_too_long(self):
        """
        Tests that a host name that is longer than 1024 characters will be
        blocked.
        """

        # create hostname
        hostname_chars = []
        for _ in range(1024):
            hostname_chars.append('h')
        hostname = ''.join(hostname_chars)

        # send PUT
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = ('put test.test1.t1 %d 0.14 host=%s\n' % (utcnow, hostname))
        self.send_message_and_close(4242, putcmd)

        # check that message was not sent
        pushdata = self.wait_for_server_command('pushdata', 60)
        self.assertTrue(pushdata.empty())

        # check log file for blocked point
        self.assert_blocked_point_in_log('WF-407:.*Host.*too long.*' +
                                         hostname, False)

    def test_metric_name_too_long(self):
        """
        Tests that a metric name that is longer than 1024 characters will be
        blocked.
        """

        # create metric name
        metric_name_chars = []
        for _ in range(1024):
            metric_name_chars.append('m')
        metric_name = ''.join(metric_name_chars)

        # send PUT
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = ('put %s %d 0.14 host=localhost\n' % (metric_name, utcnow))
        self.send_message_and_close(4242, putcmd)

        # check that message was not sent
        pushdata = self.wait_for_server_command('pushdata', 60)
        self.assertTrue(pushdata.empty())

        # check log file for blocked point
        self.assert_blocked_point_in_log('WF-408: Metric name is too long.*' +
                                         metric_name, False)

    def test_metric_invalid_characters(self):
        """
        Tests that lines are blocked when the metric names contain invalid
        characters.
        """

        # create metric names to test
        metric_names = {
            ('foo-./0123456789.ABCDEFGHIJKLMNOPQRSTUVWXYZ.'
             'abcdefghijklmnopqrstuvwxyz_'): True,
            ('~abc-./0123456789.ABCDEFGHIJKLMNOPQRSTUVWXYZ.'
             'abcdefghijklmnopqrstuvwxyz_'): True,
            '01234.ABCD.~abc': False,
            'A=BC.123': False,
            'abc7*.123': False,
            'abc(.123)': False
        }

        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        for metric, expected in metric_names.iteritems():
            print '\n*** Testing metric |%s| ***' % (metric)

            # send PUT
            putcmd = ('put %s %d 0.14 host=localhost\n' % (metric, utcnow,))
            self.send_message_and_close(4242, putcmd)

            if expected:
                # check that it was sent on to the WF server
                pushdata_expect = [("\"%s\" 0.14 %d source=\"localhost\"" %
                                    (metric, utcnow))]
                self.wait_for_push_data(pushdata_expect)

            else:
                pushdata = self.wait_for_server_command('pushdata', 60)
                self.assertTrue(pushdata.empty())
                self.assert_blocked_point_in_log(metric, True)

class TestProxyWavefrontFormat(TestAgentProxyBase):
    """
    Tests the wavefront format protocol support
    """

    def __init__(self, *args, **kwargs):
        super(TestProxyWavefrontFormat, self).__init__(*args, **kwargs)

    def setUp(self):
        self.setup_common(self.__class__.__name__ + '__' +
                          self._testMethodName, 2878)

    def tearDown(self):
        self.teardown_common()

    def test_simple_1(self):
        """
        Simple test of a single metric using wavefront format
        """

        # send metric
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = ('test.test2.t1 0.214 %d source=%s\n' % (utcnow, 'localhost'))
        self.send_message_and_close(2878, putcmd)

        # check that it was sent on to the WF server
        pushdata_expect = [("\"test.test2.t1\" 0.214 %d source=\"%s\"" %
                            (utcnow, 'localhost'))]
        self.wait_for_push_data(pushdata_expect)

    def test_simple_no_ts_1(self):
        """
        Simple test of a single metric using wavefront format without timestamp
        """

        # send metric
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = ('test.test2.t1 0.214 source=%s\n' % ('localhost', ))
        self.send_message_and_close(2878, putcmd)

        # check that it was sent on to the WF server
        pushdata_expect = [("\"test.test2.t1\" 0.214 %d source=\"%s\"" %
                            (utcnow, 'localhost'))]
        self.wait_for_push_data(pushdata_expect)

    def test_parallel_1(self):
        """
        Test of multiple commands in parallel
        """

        # send commands
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))

        expected_cmds = []
        for j in range(100):
            cmds = []
            for i in range(1000):
                cmds.append('test.test2.t%d %d.0 %d source=%s\n' %
                            (j, i, utcnow, 'localhost'))
                expected_cmds.append('"test.test2.t%d" %d.0 %d '
                                     'source="localhost"' % (j, i, utcnow))

            self.send_message_parallel(2878, cmds)
        self.wait_for_push_data(expected_cmds)

class TestProxyGraphiteFormat(TestAgentProxyBase):
    """
    Tests the regular graphite protocol support
    """

    def __init__(self, *args, **kwargs):
        super(TestProxyGraphiteFormat, self).__init__(*args, **kwargs)

    def setUp(self):
        args = [
            '--graphitePorts', '2003',
            '--graphiteFormat', '2',
        ]
        self.setup_common(self.__class__.__name__ + '__' +
                          self._testMethodName, 2003, args)

    def tearDown(self):
        self.teardown_common()

    def test_simple_1(self):
        """
        Simple test of a single metric using graphite protocol
        """

        # send metric
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = ('test.%s.test1.t1 0.204 %d\n' % ('localhost', utcnow))
        self.send_message_and_close(2003, putcmd)

        # check that it was sent on to the WF server
        pushdata_expect = [("\"test.test1.t1\" 0.204 %d source=\"%s\"" %
                            (utcnow, 'localhost'))]
        self.wait_for_push_data(pushdata_expect)

    def test_parallel_1(self):
        """
        Test of multiple commands in parallel
        """

        # send commands
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))

        expected_cmds = []
        for j in range(100):
            cmds = []
            for i in range(1000):
                cmds.append('test.%s.test1.t%d %d.0 %d\n' %
                            ('localhost', j, i, utcnow))
                expected_cmds.append('"test.test1.t%d" %d.0 %d '
                                     'source="localhost"' % (j, i, utcnow))

            self.send_message_parallel(2003, cmds)
        self.wait_for_push_data(expected_cmds)

class TestProxyGraphiteFormatWithOptionsSet(TestAgentProxyBase):
    """
    Tests the regular graphite protocol support with updated graphiteFormat
    option and a graphiteDelimters configuration option.
    """

    def __init__(self, *args, **kwargs):
        super(TestProxyGraphiteFormatWithOptionsSet, self).__init__(
            *args, **kwargs)

    def setUp(self):
        args = [
            '--graphitePorts', '2003',
            '--graphiteFormat', '3',
            '--graphiteDelimiters', '-',
        ]
        self.setup_common(self.__class__.__name__ + '__' +
                          self._testMethodName, 2003, args)

    def tearDown(self):
        self.teardown_common()

    def test_host_with_delimiters(self):
        """
        Simple test of a single metric using graphite protocol
        """

        # send metric
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = ('test.test1.%s.t1 0.204 %d\n' % ('localhost-foo-bar', utcnow))
        self.send_message_and_close(2003, putcmd)

        # check that it was sent on to the WF server
        pushdata_expect = [("\"test.test1.t1\" 0.204 %d source=\"%s\"" %
                            (utcnow, 'localhost.foo.bar'))]
        self.wait_for_push_data(pushdata_expect)

class TestProxyGraphiteFormatWithOptionsSet2(TestAgentProxyBase):
    """
    Tests the regular graphite protocol support with updated graphiteFormat
    option and graphiteFieldsToRemove
    """

    def __init__(self, *args, **kwargs):
        super(TestProxyGraphiteFormatWithOptionsSet2, self).__init__(
            *args, **kwargs)

    def setUp(self):
        args = [
            '--graphitePorts', '2003',
            '--graphiteFormat', '2',
            '--graphiteFieldsToRemove', '1'
        ]
        self.setup_common(self.__class__.__name__ + '__' +
                          self._testMethodName, 2003, args)

    def tearDown(self):
        self.teardown_common()

    def test_metric_with_fields_removed(self):
        """
        Simple test of a single metric using graphite protocol
        """

        # send metric
        utcnow = unix_time_seconds(datetime.datetime.utcnow().replace(
            tzinfo=dateutil.tz.tzutc()))
        putcmd = ('test.%s.test1.t1 0.204 %d\n' % ('localhost-foo-bar', utcnow))
        self.send_message_and_close(2003, putcmd)

        # check that it was sent on to the WF server
        pushdata_expect = [("\"test1.t1\" 0.204 %d source=\"%s\"" %
                            (utcnow, 'localhost-foo-bar'))]
        self.wait_for_push_data(pushdata_expect)

class TestPickleProtocolProxy(TestAgentProxyBase):
    """
    Tests the graphite pickle protocol support
    """

    def __init__(self, *args, **kwargs):
        super(TestPickleProtocolProxy, self).__init__(*args, **kwargs)

    def setUp(self):
        args = [
            '--graphiteFormat', '2',
            '--graphiteFieldsToRemove', '1',
        ]
        self.setup_common(self.__class__.__name__ + '__' +
                          self._testMethodName, 5878, args)

    def tearDown(self):
        self.teardown_common()

    #pylint: disable=too-many-locals
    def test_example_capture_1(self):
        """
        Test of a capture sent to us by Rocket Fuel.
        """

        # connect to the proxy on port 5878 - the pickle protocol port
        sock = socket.socket()
        sock.connect(('127.0.0.1', 5878))

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
            sock.sendall(''.join(all_data))

        # all done.  close the socket
        sock.shutdown(socket.SHUT_WR)
        sock.close()

        expected = []
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
                tstamp = int(metric[1][0]) # remove .0 with int()
                value = metric[1][1]

                orig_value = value
                precision = get_precision(orig_value)
                value_len = len(str(int(value)))
                if value_len > 7:
                    value = (('%.' + str(precision) + 'E') %
                             (value)).replace('+', '').replace('E0', 'E')

                # "metric name" value ts source=hostname
                expected.append('"%s" %s %d source="%s"' %
                                (metric_name, value, tstamp, host_name))

        self.wait_for_push_data(expected)

def get_precision(value):
    """
    This is a hacky function for getting the precision to use when comparing
    the value sent to WF via the proxy with a double value passed to it.
    Arguments:
    value - the value to return the precision to use with a %.E format string
    """

    sval = '%.2f' % (float(value))
    last_index = len(sval)
    found_decimal = False
    for index in range(len(sval) - 1, 0, -1):
        char = sval[index]
        last_index = index
        if char == '.':
            found_decimal = True
        if char != '0' and char != '.':
            break

    if not found_decimal:
        return last_index - 1
    else:
        return last_index

if __name__ == '__main__':
    unittest.main()
