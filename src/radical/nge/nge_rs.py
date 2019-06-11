
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import json
import requests

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def tolist(thing):

    if   thing is None          : return []
    elif isinstance(thing, list): return thing
    else                        : return [thing]


# ------------------------------------------------------------------------------
#
class NGE_RS(object):
    '''
    This is class interfaces to the NGE service's REST API.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, url, log=None, rep=None, prof=None):

        if log : self._log  = log
        else   : self._log  = ru.Logger('radical.nge')

        if rep : self._rep  = log
        else   : self._rep  = ru.Reporter('radical.nge')

        if prof: self._prof = prof
        else   : self._prof = ru.Profiler('radical.nge')

        self._cookies        = list()
        self._url            = ru.Url(url)

        self._qbase          = ru.Url(url)
      # self._qbase.username = None
      # self._qbase.password = None
        self._qbase          = str(self._qbase).rstrip('/')

        if self._url.username and self._url.password:
            self.login(self._url.username, self._url.password)


    # --------------------------------------------------------------------------
    #
    def _query(self, mode, route, data=None):

        url = self._qbase + route

        ldata = -1
        if data is not None:
            ldata = len(str(data))

        print '---> %-5s  %-60s [data:%d]' % (mode.upper(), url, ldata)

        self._log.debug('request %5s: %s [%s]', mode, route, data)
        self._log.debug('request %5s: %s', mode, url)

        if mode == 'get':
            r = requests.get(url, cookies=self._cookies, json=data)

        elif mode == 'put':
            r = requests.put(url, cookies=self._cookies, json=data)

        elif mode == 'post':
            r = requests.post(url, cookies=self._cookies, json=data)

        elif mode == 'delete':
            r = requests.delete(url, cookies=self._cookies, json=data)

        else:
            raise ValueError('invalid query mode %s' % mode)

        self._log.debug('reply   %3s: %s [%s]', r.status_code,
                                                 len(r.content), r.content[:64])

        if r.status_code != 200:
            raise RuntimeError('query failed:\n %s' % r.content)

        if r.cookies:
            assert(not self._cookies), 'we allow auth only once'
            self._cookies = r.cookies

        try:
            result = json.loads(r.content)

        except ValueError as e:
            raise RuntimeError('query failed: %s' % repr(e))

        print '     %-6s [%s]' % (result['success'], result.get('error', ''))
        if not result['success']:
            raise RuntimeError('query failed: %s' % result['error'])

        return result['result']



    # --------------------------------------------------------------------------
    #
    def login(self, username, password):
        '''
        login to the service with given username and password.  This method will
        stor a cookie with a session secret so that future calls on this NGE
        object instance use the same credentials.  Another call to `login` will
        overwrite that cookie and use the new credentials.
        '''

        data = {'username' : self._url.username, 
                'password' : self._url.password}

        return self._query('put', '/login/', data)


    # --------------------------------------------------------------------------
    #
    def logout(self):
        '''
        delete all sessions, terminate all pilots, invalidate the cookie.
        '''

        return self._query('put', '/logout/')


    # --------------------------------------------------------------------------
    #
    def sessions_create(self, sid):
        '''
        create named session exists.  This will raise an error if the session
        already exists.
        '''

        return self._query('put', '/sessions/%s/' % sid)


    # --------------------------------------------------------------------------
    #
    def sessions_inspect(self):
        '''
        return all session IDs known for this user (irrespective of session
        state)
        '''

        return self._query('get', '/sessions/')


    # --------------------------------------------------------------------------
    #
    def sessions_close(self, sid):
        '''
        close the given session,  terminate all pilots and tasks.
        '''

        return self._query('delete', '/sessions/%s/' % sid)


    # --------------------------------------------------------------------------
    #
    def pilots_submit(self, sid, descriptions):
        '''
        request pilots, either as backfill or batch queue pilots.  This call
        will return a list of pilot IDs.
        '''

        if   not descriptions                  : descriptions = list()
        elif not isinstance(descriptions, list): descriptions = [descriptions]

        return self._query('put', '/sessions/%s/pilots/' % sid, descriptions)


    # --------------------------------------------------------------------------
    #
    def pilots_inspect(self, sid, pids=None):
        '''
        return information about all pilots
        '''

        pids = tolist(pids)

        if pids and len(pids) == 1 and pids[0]:
            return self._query('get', '/sessions/%s/pilots/%s' % (sid, pids[0]))
        else:
            if pids: data = {'pids': pids}
            else   : data = {}
            return self._query('get', '/sessions/%s/pilots/' % sid, data)


    # --------------------------------------------------------------------------
    #
    def pilots_wait(self, sid, pids=None, states=None, timeout=None):
        '''
        wait for a specific (set of) states for all pilots with the given UIDs
        (or for all known resources if no UID is specified).  This call will
        return after a given timeout, or after any of the given states have been
        reached, whichever occurs first.  A negative timeout value will cause it
        to wait forever.
        '''

        data = {'pids'   : tolist(pids),
                'states' : tolist(states),
                'timeout': timeout}

        if pids and len(pids) == 1 and pids[0]:
            self._query('post', '/sessions/%s/pilots/%s/' % (sid, pids[0]),
                        data)
        else:
            self._query('post', '/sessions/%s/pilots/' % sid, data)

        return


    # --------------------------------------------------------------------------
    #
    def pilots_cancel(self, sid, pids=None):
        '''
        cancel all resources (ie. RP pilots) with the given UIDs (or for all
        known resources if no UID is specified).  This call will return when the
        resource states are final.
        '''

        if pids and len(pids) == 1 and pids[0]:
            self._query('delete', '/sessions/%s/pilots/%s' % (sid, pids[0]))
        else:
            data = {'pids': pids}
            self._query('delete', '/sessions/%s/pilots/' % sid, data)

        return


    # --------------------------------------------------------------------------
    #
    def tasks_submit(self, sid, descriptions):
        '''
        Harvester task descriptions are submitted to the RP level resources
        (pilots).
        '''

        if not descriptions:
            return []

        data = {'descriptions': tolist(descriptions)}

        return self._query('put', '/sessions/%s/tasks/' % sid, data)


    # --------------------------------------------------------------------------
    #
    def tasks_inspect(self, sid, tids=None):
        '''
        return UIDs for all known tasks (ie. RP units)
        '''

        if tids and len(tids) == 1 and tids[0]:
            return self._query('get', '/sessions/%s/tasks/%s' % (sid, tids[0]))
        else:
            if tids: data = {'pids': tids}
            else   : data = {}
            return self._query('get', '/sessions/%s/tasks/' % sid, data)


    # --------------------------------------------------------------------------
    #
    def tasks_stdout(self, sid, tid):
        '''
        return the stdout of a completed task
        '''

        return self._query('get', '/sessions/%s/tasks/%s/stdout' % (sid, tid))


    # --------------------------------------------------------------------------
    #
    def tasks_stderr(self, sid, tid):
        '''
        return the stderr of a completed task
        '''

        return self._query('get', '/sessions/%s/tasks/%s/stderr' % (sid, tid))


    # --------------------------------------------------------------------------
    #
    def tasks_wait(self, sid, tids=None, states=None, timeout=None):
        '''
        wait for a specific (set of) states for all tasks (ie. RP units)
        with the given UIDs (or for all known tasks if no UID is specified).
        This call will return after a given timeout, or after the states have
        been reached, whichever occurs first.  A negative timeout value will
        cause it to wait forever.
        '''

        data = {'tids'   : tolist(tids),
                'states' : tolist(states),
                'timeout': timeout}

        if tids and len(tids) == 1 and tids[0]:
            self._query('post', '/sessions/%s/tasks/%s/' % (sid, tids[0]),
                        data)
        else:
            self._query('post', '/sessions/%s/tasks/' % sid, data)

        return


    # --------------------------------------------------------------------------
    #
    def tasks_inspect(self, sid, tids=None):
        '''
        return UIDs for all known tasks (ie. RP units)
        '''

        if tids and len(tids) == 1 and tids[0]:
            return self._query('get', '/sessions/%s/tasks/%s' % (sid, tids[0]))
        else:
            if tids: data = {'pids': tids}
            else   : data = {}
            return self._query('get', '/sessions/%s/tasks/' % sid, data)


# ------------------------------------------------------------------------------

