#!/usr/bin/env python

__copyright__ = 'Copyright 2017-19, http://radical.rutgers.edu'
__license__   = 'MIT'


# --------------------------------------------------------------------------
#
# see https://docs.google.com/document/d/ \
#                             1bm8ucgfi9SHjDy0w-ZX5NIdkjk87qFClMB9jMse75uM
#
import os
import json
import bottle

import radical.utils as ru
import radical.nge   as rn

os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://nge:nge@two.radical-project.org/nge'


# ------------------------------------------------------------------------------
# https://stackoverflow.com/questions/8725605/
def methodroute(route, **kwargs):
    def decorator(f):
        if not hasattr(f, 'routes'):
            f.routes = list()
        f.routes.append(route)
        for arg in kwargs:
            setattr(f, arg, kwargs[arg])
        return f
    return decorator


# ------------------------------------------------------------------------------
#
def routeapp(obj):
    for kw in dir(obj):
        attr = getattr(obj, kw)
        if hasattr(attr, 'routes'):
            for route in attr.routes:
                if hasattr(attr, 'method'):
                    method = getattr(attr, 'method')
                else:
                    method = 'GET'
                if hasattr(attr, 'callback'):
                    callback = getattr(attr, 'callback')
                else:
                    callback = None
                if hasattr(attr, 'name'):
                    name = getattr(attr, 'name')
                else:
                    name = None
                if hasattr(attr, 'apply'):
                    aply = getattr(attr, 'apply')
                else:
                    aply = None
                if hasattr(attr, 'skip'):
                    skip = getattr(attr, 'skip')
                else:
                    skip = None

                bottle.route(route, method, callback, name, aply, skip)(attr)


# ------------------------------------------------------------------------------
#
class _Account(dict):

    def __init__(self, username, password):

        super(dict, self).__init__()

        self['username'] = username
        self['password'] = password
        self['sessions'] = dict()
        self['secret'  ] = None


# ------------------------------------------------------------------------------
#
class NGE_Server(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        '''
        initialize the service endpoint:

          - create logger, profile and reporter
          - set up accounts
        '''

        self._log      = ru.Logger  ('radical.nge.service')
        self._rep      = ru.Reporter('radical.nge.service')
        self._prof     = ru.Profiler('radical.nge.service')
        self._accounts = {
                             'andre' : _Account('andre' , 'erdna' ),
                             'matteo': _Account('matteo', 'eottam'),
                             'daniel': _Account('daniel', 'leinad'), 
                             'guest' : _Account('guest' , 'guest' ),
                         }

        self._rep.header('--- NGE (%s) ---' % rn.version)


    # --------------------------------------------------------------------------
    #
    def _serve(self):
        '''
        Open this service endpoint and begin serving requests
        '''

        routeapp(self)

        port = int(os.environ.get('RADICAL_NGE_PORT', 8090))
        host = str(os.environ.get('RADICAL_NGE_HOST', '0.0.0.0'))

        self._rep.info('serve on http://%s:%d/\n\n' % (host, port))
        bottle.run(host=host, port=port, debug=True, quiet=False)


    # --------------------------------------------------------------------------
    #
    def _terminate(self):
        '''
        Close this service endpoint

          - close all sessions for all users (which frees all pilots)
          - stop listening on the service port
        '''

        # close all open sessions
        for user in self._accounts:
            for sid in self._accounts[user]['sessions']:
                try:
                    self._accounts[user]['sessions'][sid].close()
                except:
                    pass
            self._accounts[user]['sessions'] = dict()


    # --------------------------------------------------------------------------
    #
    def _check_cookie(self, request):
        '''
        Check if the given request carries a cookie and if this cookie is
        associated with a user account.  If it is, return the respective account
        record.
        '''

        username = request.get_cookie('username')
        account  = self._get_account(username)
        secret   = account['secret']
        check    = request.get_cookie('secret', secret=secret)

        if not check or check != username:
            raise RuntimeError('invalid session (%s != %s)' % (check, username))

        account['username'] = username

        return account


    # --------------------------------------------------------------------------
    #
    def _get_account(self, username):
        '''
        Check if given username is known and return the full account record
        '''

        if username not in self._accounts:
            raise ValueError('invalid username [%s]' % username)

        return self._accounts[username]


    # --------------------------------------------------------------------------
    #
    def _get_session(self, account, sid):
        '''
        Check if a session exists and return it
        '''

        if sid not in account['sessions']:
            raise ValueError('session %s does not exist' % sid)

        return account['sessions'][sid]


    # --------------------------------------------------------------------------
    #
    @methodroute('/login/', method='PUT')
    def login(self):
        '''
        Connect to the service.

        This expects json data of the form:

            {
                 'username' : 'foo', 
                 'password' : 'bar'
            }

        The response will contain a cookie which must be used for subsequent
        requests to this service endpoint.  The cookie is valid until `logout`
        is called.
        '''

        self._log.info('login')
        try:
            data = json.loads(bottle.request.body.read())

            username = data.get('username')
            password = data.get('password')
            account  = self._get_account(username)

            self._log.info('login %s', username)

            if account['password'] != password:
                raise RuntimeError('invalid password')

            # create a new cookie secret if needed
            if 'secret' in account:
                secret = account['secret']
            else:
                secret = ru.generate_id('nge.secret', mode=ru.ID_UUID)
                account['secret'] = secret

            bottle.response.set_cookie('username', username, path='/')
            bottle.response.set_cookie('secret',   username, path='/',
                                       secret=secret)

            return {'success' : True,
                    'result'  : None}

        except Exception as e:
            self._log.exception('login failed')
            print 'login failed: %s' % e
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/logout/', method='PUT')
    def logout(self):
        '''
        This method will invalidate the session cookie, and all further
        operations (apart from a new login) will cause an error.  

        On logout, all sessions for the user will be closed, all pilots will be
        terminated.
        '''

        try:
            account = self._check_cookie(bottle.request)

            self._log.info('logout %s', account['username'])

            # close all sessions for this user
            for sid in account['sessions']:
                account['sessions'][sid].close

            account['sessions'] = dict()

            return {'success' : True,
                    'result'  : None}

        except Exception as e:
            self._log.exception('logout failed')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/', method='PUT')
    def sessions_create(self, sid):
        '''
        For any user (login), several `sessions` can coexist.  A session is here
        defined as a set of pilot resources and tasks.  A `PUT` on this route
        will create such a session.

        The call will raise an error if the session exists.
        '''

        try:
            account = self._check_cookie(bottle.request)
            session = None

            if sid in account['sessions']:
                raise ValueError('session %s exists' %  sid)

            session = rn.NGE_RP(self._rep, self._log, self._prof)
            account['sessions'][sid] = session

            return {'success' : True,
                    'result'  : None}

        except Exception as e:
            self._log.exception('session crceation failed')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/', method='GET')
    def sessions_inspect(self):
        '''
        List all known session IDs for the current user
        '''

        try:
            account = self._check_cookie(bottle.request)

            return {'success' : True,
                    'result'  : account['sessions'].keys()}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/',       method='DELETE')
    @methodroute('/sessions/<sid>/', method='DELETE')
    def sessions_close(self, sid=None):
        '''
        Close the session identified by `sid`.  This will terminate all pilots
        and tasks started in this session.  If no SID is given, close all
        sessions for this user.
        '''

        try:
            account = self._check_cookie(bottle.request)

            if sid:
                # delete session with given ID
                if sid not in account['sessions']:
                    raise ValueError('session %s does not exist' % sid)
                account['sessions'][sid].close()
                del(account['sessions'][sid])

            else:
                # delete all of them
                for sid in account['sessions']:
                    account['sessions'][sid].close()
                account['sessions'] = dict()

            return {'success' : True,
                    'result'  : None}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    # Pilots
    #
    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/pilots/', method='PUT')
    def pilots_submit(self, sid):
        '''
        Submit a list of pilots.  This expectes a list of pilot descriptions as
        json data.  Tyhe return value will be a list of pilot UIDs which can be
        used to control the submitted pilot jobs.
        '''

        try:
            account = self._check_cookie(bottle.request)
            session = self._get_session(account, sid)
            data    = json.loads(bottle.request.body.read())
            retval  = session.pilots_submit(data)

            return {'success' : True,
                    'result'  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/pilots/<pid>/', method='GET')
    @methodroute('/sessions/<sid>/pilots/',       method='GET')
    def pilots_inspect(self, sid, pid=None):
        '''
        This method will inspect one or multiple pilots, returning no
        '''

        try:
            account = self._check_cookie(bottle.request)
            session = self._get_session(account, sid)

            if pid:
                pids = [pid]
            else:
                data = json.loads(bottle.request.body.read())
                pids = data.get('pids')

            retval = session.pilots_inspect(pids)

            return {'success' : True,
                    'result'  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/pilots/<pid>/', method='POST')
    @methodroute('/sessions/<sid>/pilots/',       method='POST')
    def pilots_wait(self, sid, pid=None):

        try:
            account = self._check_cookie(bottle.request)
            session = self._get_session(account, sid)
            data    = json.loads(bottle.request.body.read())

            if pid:
                pids = [pid]
            else:
                pids = data.get('pids')

            states  = data.get('states')
            timeout = data.get('timeout')

            retval  = session.pilots_wait(pids, states, timeout)

            return {'success' : True,
                    'result'  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/pilots/<pids>', method='DELETE')
    @methodroute('/sessions/<sid>/pilots/',       method='DELETE')
    def pilots_cancel(self, sid, pids=None):

        try:
            account = self._check_cookie(bottle.request)
            session = self._get_session(account, sid)
            retval  = session.pilots_cancel(pids)

            return {'success' : True,
                    'result'  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    # Tasks
    #
    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/tasks/', method='PUT')
    def tasks_submit(self, sid):

        try:
            account = self._check_cookie(bottle.request)
            session = self._get_session(account, sid)
            data    = json.loads(bottle.request.body.read())

            retval  = session.tasks_submit(data['descriptions'])

            return {'success' : True,
                    'result'  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/tasks/<tid>/', method='GET')
    @methodroute('/sessions/<sid>/tasks/',       method='GET')
    def tasks_inspect(self, sid, tid=None):

        try:
            account = self._check_cookie(bottle.request)
            session = self._get_session(account, sid)

            if tid:
                tids = [tid]
            else:
                data = json.loads(bottle.request.body.read())
                tids = data.get('tids')

            retval = session.tasks_inspect(tids)

            return {'success' : True,
                    'result'  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/tasks/<tid>/stdout', method='GET')
    def tasks_stdout(self, sid, tid):

        try:
            account = self._check_cookie(bottle.request)
            session = self._get_session(account, sid)

            retval  = session.tasks_stdout(tid)

            return {'success' : True,
                    'result'  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/tasks/<tid>/stderr', method='GET')
    def tasks_stderr(self, sid, tid):

        try:
            account = self._check_cookie(bottle.request)
            session = self._get_session(account, sid)

            retval  = session.tasks_stderr(tid)

            return {'success' : True,
                    'result'  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/sessions/<sid>/tasks/<task_id>/', method='POST')
    @methodroute('/sessions/<sid>/tasks/',           method='POST')
    def tasks_wait(self, sid, tid=None):

        try:
            account = self._check_cookie(bottle.request)
            session = self._get_session(account, sid)

            if tid: tids = [tid]
            else  : tids = None

            data    = json.loads(bottle.request.body.read())

            states  = data.get('states')
            timeout = data.get('timeout')
            retval  = session.tasks_wait(tids, states, timeout)

            return {'success' : True,
                    'result'  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {'success' : False,
                    'error'   : repr(e)}


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    server = None
    try:
        server = NGE_Server()
        server._serve()

    finally:
        if server:
            server._terminate()


# ------------------------------------------------------------------------------

