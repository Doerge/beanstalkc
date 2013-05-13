#!/usr/bin/env python
"""beanstalkc - A beanstalkd Client Library for Python"""

__license__ = '''
Copyright (C) 2008-2012 Andreas Bolka

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

__version__ = '0.3.0'

import logging
import socket
import random
import time


DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 11300
DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120


class BeanstalkcException(Exception): pass
class UnexpectedResponse(BeanstalkcException): pass
class CommandFailed(BeanstalkcException): pass
class DeadlineSoon(BeanstalkcException): pass

class SocketError(BeanstalkcException):
    @staticmethod
    def wrap(wrapped_function, *args, **kwargs):
        try:
            return wrapped_function(*args, **kwargs)
        except socket.error, err:
            raise SocketError(err)

class Pool(object):
    def __init__(self,bstalks):
        self.bstalks = bstalks
        self.connections = []
        self.connect()
        
        # Gather the sockets in a dict as tuples of (socket,connection) so we
        # can resolve socket objects to their connections.
        self._sockets = dict()
        for conn in self.connections:
            self._sockets[conn._socket] = conn
    
    def _call_wrap(self,conn,func,args):
        if (args == None):
            return func(conn)
        else:
            return func(conn, args )
    
    def _send_to_rand_conn(self,func,args=None):
        """Send to a random connection in the pool. Call like this:
        self._send_to_rand_conn(some_func, ) if there are no arguments to the
        function. Returns a tuple of the connection the command was sent to,
        and its response."""
        conn = random.choice(self.connections)
        resp = self._call_wrap(conn,func,args)
        return (conn,resp)
    
    def _send_to_all(self,func,args=None):
        """Send to all connections in the pool. Returns a list of tuples
        (conn,response)"""
        results = []
        for conn in self.connections:
            results.append( self._call_wrap(conn,func,args) )
        return results
    
    def reserve(self,pool_timeout=None,timeout=10):
        """Reserves a job from the pool.
        pool_timeout: Overall time in seconds before this call returns.
             timeout: Individual reserve-calls timeouts."""
        timer = None
        if (pool_timeout != None):
            timer = time.time()
        while (True):
            conn = random.choice(self.connections)
            try:
                result = self._call_wrap(conn, Connection.reserve, timeout)
                # If result == None, the reserve timed-out.
                if (result != None):
                    return (conn,result)
            except DeadlineSoon as e:
                pass
            if (timer != None and time.time() > timer + pool_timeout):
                break
        # If we get here, the pool-timeout has been reached. Returning None
        # just as Connection.reserve when the timeout is reached.
        return (None,None)
        
    def connect(self):
        for (host,port) in self.bstalks:
            try:
                conn = Connection(host=host, port=port, parse_yaml=True,
                                  connect_timeout=socket.getdefaulttimeout())
                self.connections.append( conn )
            except SocketError, e:
                logging.error('Failed connecting to %s %d' % (host,port))
    
    def close(self):
        """Close all connections in the pool."""
        self._send_to_all( Connection.close)
    
    def put(self,arg):
        """Put a job in the pool."""
        return self._send_to_rand_conn( Connection.put, arg)
    
    def using(self):
        """Return the tubes currently in use for every connection in the
        pool."""
        return self._send_to_all( Connection.use, name)
    
    def use(self,name):
        """Use this tube on every connection in the pool."""
        self._send_to_all( Connection.use, name)
    
    def watching(self):
        """Return a list of all tubes being watched."""
        return self._send_to_all( Connection.watching)

    def watch(self, name):
        """Watch a given tube in all connections."""
        return self._send_to_all( Connection.watch, name)
    
    def ignore(self,name):
        """Ignore the given tube in all connections in the pool."""
        self._send_to_all( Connection.ignore, name)
    
    def stats(self):
        """Return a list of dicts of beanstalkd statistics."""
        return self._send_to_all( Connection.stats)

    def stats_tube(self, name):
        """Return a list of dicts of stats about a given tube."""
        return self._send_to_all( Connection.stats_tube, name)
        
    def pause_tube(self, name, delay):
        """Pause a tube on all connections for a given delay time, in
        seconds."""
        self._send_to_all( Connection.pause_tube,[name, delay])
    

class Connection(object):
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, parse_yaml=True,
                 connect_timeout=socket.getdefaulttimeout()):
        if parse_yaml is True:
            try:
                parse_yaml = __import__('yaml').load
            except ImportError:
                logging.error('Failed to load PyYAML, will not parse YAML')
                parse_yaml = False
        self._connect_timeout = connect_timeout
        self._parse_yaml = parse_yaml or (lambda x: x)
        self.host = host
        self.port = port
        self.connect()

    def connect(self):
        """Connect to beanstalkd server."""
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self._connect_timeout)
        SocketError.wrap(self._socket.connect, (self.host, self.port))
        self._socket.settimeout(None)
        self._socket_file = self._socket.makefile('rb')

    def close(self):
        """Close connection to server."""
        try:
            self._socket.sendall('quit\r\n')
            self._socket.close()
        except socket.error:
            pass

    def reconnect(self):
        """Re-connect to server."""
        self.close()
        self.connect()

    def _interact(self, command, expected_ok, expected_err=[]):
        SocketError.wrap(self._socket.sendall, command)
        status, results = self._read_response()
        if status in expected_ok:
            return results
        elif status in expected_err:
            raise CommandFailed(command.split()[0], status, results)
        else:
            raise UnexpectedResponse(command.split()[0], status, results)

    def _read_response(self):
        line = SocketError.wrap(self._socket_file.readline)
        if not line:
            raise SocketError()
        response = line.split()
        return response[0], response[1:]

    def _read_body(self, size):
        body = SocketError.wrap(self._socket_file.read, size)
        SocketError.wrap(self._socket_file.read, 2)  # trailing crlf
        if size > 0 and not body:
            raise SocketError()
        return body

    def _interact_value(self, command, expected_ok, expected_err=[]):
        return self._interact(command, expected_ok, expected_err)[0]

    def _interact_job(self, command, expected_ok, expected_err, reserved=True):
        jid, size = self._interact(command, expected_ok, expected_err)
        body = self._read_body(int(size))
        return Job(self, int(jid), body, reserved)

    def _interact_yaml(self, command, expected_ok, expected_err=[]):
        size, = self._interact(command, expected_ok, expected_err)
        body = self._read_body(int(size))
        return self._parse_yaml(body)

    def _interact_peek(self, command):
        try:
            return self._interact_job(command, ['FOUND'], ['NOT_FOUND'], False)
        except CommandFailed, (_, _status, _results):
            return None

    # -- public interface --

    def put(self, body, priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
        """Put a job into the current tube. Returns job id."""
        assert isinstance(body, str), 'Job body must be a str instance'
        jid = self._interact_value(
                'put %d %d %d %d\r\n%s\r\n' %
                    (priority, delay, ttr, len(body), body),
                ['INSERTED', 'BURIED'], ['JOB_TOO_BIG'])
        return int(jid)

    def reserve(self, timeout=None):
        """Reserve a job from one of the watched tubes, with optional timeout
        in seconds. Returns a Job object, or None if the request times out."""
        if timeout is not None:
            command = 'reserve-with-timeout %d\r\n' % timeout
        else:
            command = 'reserve\r\n'
        try:
            return self._interact_job(command,
                                      ['RESERVED'],
                                      ['DEADLINE_SOON', 'TIMED_OUT'])
        except CommandFailed, (_, status, results):
            if status == 'TIMED_OUT':
                return None
            elif status == 'DEADLINE_SOON':
                raise DeadlineSoon(results)

    def kick(self, bound=1):
        """Kick at most bound jobs into the ready queue."""
        return int(self._interact_value('kick %d\r\n' % bound, ['KICKED']))

    def kick_job(self, jid):
        """Kick a specific job into the ready queue."""
        self._interact('kick-job %d\r\n' % jid, ['KICKED'], ['NOT_FOUND'])

    def peek(self, jid):
        """Peek at a job. Returns a Job, or None."""
        return self._interact_peek('peek %d\r\n' % jid)

    def peek_ready(self):
        """Peek at next ready job. Returns a Job, or None."""
        return self._interact_peek('peek-ready\r\n')

    def peek_delayed(self):
        """Peek at next delayed job. Returns a Job, or None."""
        return self._interact_peek('peek-delayed\r\n')

    def peek_buried(self):
        """Peek at next buried job. Returns a Job, or None."""
        return self._interact_peek('peek-buried\r\n')

    def tubes(self):
        """Return a list of all existing tubes."""
        return self._interact_yaml('list-tubes\r\n', ['OK'])

    def using(self):
        """Return the tube currently being used."""
        return self._interact_value('list-tube-used\r\n', ['USING'])

    def use(self, name):
        """Use a given tube."""
        return self._interact_value('use %s\r\n' % name, ['USING'])

    def watching(self):
        """Return a list of all tubes being watched."""
        return self._interact_yaml('list-tubes-watched\r\n', ['OK'])

    def watch(self, name):
        """Watch a given tube."""
        return int(self._interact_value('watch %s\r\n' % name, ['WATCHING']))

    def ignore(self, name):
        """Stop watching a given tube."""
        try:
            return int(self._interact_value('ignore %s\r\n' % name,
                                            ['WATCHING'],
                                            ['NOT_IGNORED']))
        except CommandFailed:
            return 1

    def stats(self):
        """Return a dict of beanstalkd statistics."""
        return self._interact_yaml('stats\r\n', ['OK'])

    def stats_tube(self, name):
        """Return a dict of stats about a given tube."""
        return self._interact_yaml('stats-tube %s\r\n' % name,
                                   ['OK'],
                                   ['NOT_FOUND'])

    def pause_tube(self, name, delay):
        """Pause a tube for a given delay time, in seconds."""
        self._interact('pause-tube %s %d\r\n' % (name, delay),
                       ['PAUSED'],
                       ['NOT_FOUND'])

    # -- job interactors --

    def delete(self, jid):
        """Delete a job, by job id."""
        self._interact('delete %d\r\n' % jid, ['DELETED'], ['NOT_FOUND'])

    def release(self, jid, priority=DEFAULT_PRIORITY, delay=0):
        """Release a reserved job back into the ready queue."""
        self._interact('release %d %d %d\r\n' % (jid, priority, delay),
                       ['RELEASED', 'BURIED'],
                       ['NOT_FOUND'])

    def bury(self, jid, priority=DEFAULT_PRIORITY):
        """Bury a job, by job id."""
        self._interact('bury %d %d\r\n' % (jid, priority),
                       ['BURIED'],
                       ['NOT_FOUND'])

    def touch(self, jid):
        """Touch a job, by job id, requesting more time to work on a reserved
        job before it expires."""
        self._interact('touch %d\r\n' % jid, ['TOUCHED'], ['NOT_FOUND'])

    def stats_job(self, jid):
        """Return a dict of stats about a job, by job id."""
        return self._interact_yaml('stats-job %d\r\n' % jid,
                                   ['OK'],
                                   ['NOT_FOUND'])


class Job(object):
    def __init__(self, conn, jid, body, reserved=True):
        self.conn = conn
        self.jid = jid
        self.body = body
        self.reserved = reserved

    def _priority(self):
        stats = self.stats()
        if isinstance(stats, dict):
            return stats['pri']
        return DEFAULT_PRIORITY

    # -- public interface --

    def delete(self):
        """Delete this job."""
        self.conn.delete(self.jid)
        self.reserved = False

    def release(self, priority=None, delay=0):
        """Release this job back into the ready queue."""
        if self.reserved:
            self.conn.release(self.jid, priority or self._priority(), delay)
            self.reserved = False

    def bury(self, priority=None):
        """Bury this job."""
        if self.reserved:
            self.conn.bury(self.jid, priority or self._priority())
            self.reserved = False

    def kick(self):
        """Kick this job alive."""
        self.conn.kick_job(self.jid)

    def touch(self):
        """Touch this reserved job, requesting more time to work on it before
        it expires."""
        if self.reserved:
            self.conn.touch(self.jid)

    def stats(self):
        """Return a dict of stats about this job."""
        return self.conn.stats_job(self.jid)


if __name__ == '__main__':
    import nose
    nose.main(argv=['nosetests', '-c', '.nose.cfg'])
