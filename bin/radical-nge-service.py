#!/usr/bin/env python

__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import json
import bottle

import radical.utils as ru
import radical.nge   as rn

accounts = {'andre'  : {'password': 'erdna' , 'session' : None,  'secret': None},
            'matteo' : {'password': 'eottam', 'session' : None,  'secret': None},
            'daniel' : {'password': 'leinad', 'session' : None,  'secret': None}, 
            'guest'  : {'password': 'guest' , 'session' : None,  'secret': None}}


# ------------------------------------------------------------------------------
# https://stackoverflow.com/questions/8725605/
def methodroute(route, **kwargs):
    def decorator(f):
        f.route = route
        for arg in kwargs:
            setattr(f, arg, kwargs[arg])
        return f
    return decorator


# ------------------------------------------------------------------------------
#
def routeapp(obj):
    for kw in dir(obj):
        attr = getattr(obj, kw)
        if hasattr(attr, "route"):
            if hasattr(attr, "method"):
                method = getattr(attr, "method")
            else:
                method = "GET"
            if hasattr(attr, "callback"):
                callback = getattr(attr, "callback")
            else:
                callback = None
            if hasattr(attr, "name"):
                name = getattr(attr, "name")
            else:
                name = None
            if hasattr(attr, "apply"):
                aply = getattr(attr, "apply")
            else:
                aply = None
            if hasattr(attr, "skip"):
                skip = getattr(attr, "skip")
            else:
                skip = None

            bottle.route(attr.route, method, callback, name, aply, skip)(attr)


# ------------------------------------------------------------------------------
#
class NGE_Server(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._log     = ru.Logger('radical.nge', level='DEBUG')
        self._rep     = ru.Reporter('radical.nge')
        self._rep.header('--- NGE (%s) ---' % rn.version)


    # --------------------------------------------------------------------------
    #
    def _start(self, account):

        if account['session']:
            # session is active
            self._log.info('session is active')
            return

        username = None
        for _username in accounts:
            if accounts[_username]['secret'] == account['secret']:
                username = _username
                break

        if not username:
            raise RuntimeError('outdated username - go away!')

        self._log.info('new session for %s', username)

        account['session'] = rn.NGE(binding=rn.RP, reporter=self._rep)


    # --------------------------------------------------------------------------
    #
    def _stop(self, account):

        if not account['session']:
            # session was closed (or never started)
            self._log.info('session is closed')
            return

        acccount['session'].close()
        acccount['session'] = None
        self._log.info('closed')


    # --------------------------------------------------------------------------
    #
    @methodroute('/login/', method="PUT")
    def login(self):

        self._log.info('login')
        try:
            data = json.loads(bottle.request.body.read())

            username = data.get('username')
            password = data.get('password')
            account  = accounts.get(username)

            self._log.info('login %s', username)

            if not account or account['password'] != password:
                raise RuntimeError('invalid username/password - go away!')

            # create cookie secret
            if not account['secret']:
                secret = ru.generate_id('NGE', mode=ru.ID_UUID)
                account['secret'] = secret

            secret = account['secret']
            bottle.response.set_cookie("username", username, path='/')
            bottle.response.set_cookie("secret",   username, path='/', secret=secret)

            # start session for this user
            self._start(account)

            return {"success" : True,
                    "result"  : username}

        except Exception as e:
            self._log.exception('unknown user')
            return {"success" : False,
                    "error"   : repr(e)}


    # --------------------------------------------------------------------------
    #
    def check_cookie(self, request):

        username = request.get_cookie("username")

        if not username or username not in accounts:
            raise RuntimeError('invalid AAA session')

        secret = accounts[username]['secret']
        check = request.get_cookie("secret", secret=secret)

        if not check or check != username:
            raise RuntimeError('invalid AAA session (%s != %s)' % (check, username))

        account = accounts.get(username)

        if not account:
            raise RuntimeError('bogus AAA session')

        return account


    # --------------------------------------------------------------------------
    #
    @methodroute('/close/', method="PUT")
    def close(self):

        try:
            account = self.check_cookie(bottle.request)

            self._rep.header('Server terminates\n\n')
            self._stop(account)
            return {"success" : True,
                    "result"  : None}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    "error"   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/restart/', method="PUT")
    def restart(self):

        try:
            account = self.check_cookie(bottle.request)

            self._rep.header('Server restarts\n\n')
            self._stop(account)
            self._start(account)
            self._log.info('restarted')
            return {"success" : True,
                    "result"  : None}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    "error"   : repr(e)}


  # # --------------------------------------------------------------------------
  # #
  # def _is_alive(self):
  #
  #     if self._closed:
  #         raise RuntimeError('session closed')
  #
  #
    # --------------------------------------------------------------------------
    #
    def serve(self):

      # for key in sorted(os.environ.keys()):
      #     print '%-20s: %s'  % (key, os.environ.get(key))

        port = int(os.environ.get('RADICAL_NGE_PORT', 8090))
        host = str(os.environ.get('RADICAL_NGE_HOST', '0.0.0.0'))

        self._rep.info('serve on http://%s:%d/\n\n' % (host, port))
        bottle.run(host=host, port=port, debug=False, quiet=True)


    # --------------------------------------------------------------------------
    #
    @methodroute('/uid/', method="GET")
    def uid(self):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].uid
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    "error"   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/backfill/<partition>/<policy>/', method="PUT")
    def request_backfill_resources(self, partition, policy):

        request_stub = json.loads(bottle.request.body.read())

        try:
            account = self.check_cookie(bottle.request)

            PWD = os.path.dirname(rn.__file__)
          # print '%s/policies/%s.json' % (PWD, policy)
            pol = ru.read_json('%s/policies/%s.json' % (PWD, policy))
          # print pol
            retval  = account['session'].request_backfill_resources(request_stub,
                                                           partition, pol)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
          # print e
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/', method="PUT")
    def request_resources(self):

        requests = json.loads(bottle.request.body.read())

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].request_resources(requests)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/', method="GET")
    def list_resources(self):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].list_resources()
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/<states>', method="GET")
    def find_resources(self, states=None):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].find_resources(states)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/requested', method="GET")
    def get_requested_resources(self):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].get_requested_resources()
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/available', method="GET")
    def get_available_resources(self):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].get_available_resources()
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/<resource_ids>/info', method="GET")
    def get_resource_info(self, resource_ids):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].get_resource_info(resource_ids)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/<resource_ids>/state', method="GET")
    def get_resource_states(self, resource_ids):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].get_resource_states(resource_ids)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/<resource_ids>/wait/<states>/<timeout>', method="GET")
    def wait_resource_states(self, resource_ids, states, timeout):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].wait_resource_states(resource_ids, states, timeout)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/resources/<resource_ids>/cancel', method="GET")
    def cancel_resources(self, resource_ids):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].cancel_resources(resource_ids)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/tasks/', method="GET")
    def list_tasks(self):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].list_tasks()
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/tasks/', method="PUT")
    def submit_tasks(self):

        descriptions = json.loads(bottle.request.body.read())

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].submit_tasks(descriptions)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/tasks/<task_ids>/state', method="GET")
    def get_task_states(self, task_ids):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].get_task_states(task_ids)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/tasks/state', method="GET")
    def get_tasks_states(self):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].get_task_states()
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/tasks/<task_ids>/wait/<states>/<timeout>', method="GET")
    def wait_task_states(self, task_ids, states, timeout):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].wait_task_states(task_ids, states, timeout)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


    # --------------------------------------------------------------------------
    #
    @methodroute('/tasks/wait/<states>/<timeout>', method="GET")
    def wait_tasks_states(self, states, timeout):

        try:
            account = self.check_cookie(bottle.request)
            retval  = account['session'].wait_task_states(task_ids=None,
                                                          states=states,
                                                          timeout=timeout)
            return {"success" : True,
                    "result"  : retval}

        except Exception as e:
            self._log.exception('oops')
            return {"success" : False,
                    'error'   : repr(e)}


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    server = None
    try:
        server = NGE_Server()
        routeapp(server)
        server.serve()

    finally:
        if server:
            server.close()


# ------------------------------------------------------------------------------

