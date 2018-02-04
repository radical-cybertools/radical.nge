
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from .nge import NGE

import os
import json
import time
import pprint
import requests

import radical.utils as ru


# --------------------------------------------------------------------------
#
# see https://docs.google.com/document/d/1bm8ucgfi9SHjDy0w-ZX5NIdkjk87qFClMB9jMse75uM
#
class NGE_RPS(NGE):
    '''
    This is the RPS bound implementation of the abstract NGE class, which
    queries a nge server instance via REST
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, url, reporter=None):

        self._url     = url.strip('/')
        self._rep     = reporter
        self._cookies = list()


    # --------------------------------------------------------------------------
    #
    def _query(self, mode, route, data=None):

        if mode == 'get':
            r = requests.get(self._url + route, cookies=self._cookies)

        elif mode == 'put':
            r = requests.put(self._url + route, cookies=self._cookies, json=data)

        else:
            raise ValueError('invalid query mode %s' % mode)


        if r.cookies:
            assert(not self._cookies), 'we allow auth only once'
            self._cookies = r.cookies

        try:
            result = json.loads(r.content)

        except ValueError as e:
            raise RuntimeError('query failed: %s' % repr(e))

        if not result['success'] or r.status_code is not 200:
            raise RuntimeError('query failed: %s' % result['error'])

        return result['result']


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):

        return self._query('get', '/uid/')


    # --------------------------------------------------------------------------
    #
    def login(self, username, password):

        data = {'username' : username, 
                'password' : password}

        return self._query('put', '/login/', data=data)


    # --------------------------------------------------------------------------
    #
    def close(self):

        return
      # return self._query('put', '/close/')


    # --------------------------------------------------------------------------
    #
    def request_backfill_resources(self, request_stub, partition, policy):

        return self._query('put', '/resources/backfill/%s/%s/' % 
                           (partition, policy), data=request_stub)


    # --------------------------------------------------------------------------
    #
    def request_resources(self, requests):

        if   not requests                  : requests = list()
        elif not isinstance(requests, list): requests = [requests]

        return self._query('put', '/resources/', data=requests)


    # --------------------------------------------------------------------------
    #
    def list_resources(self):

        return self._query('get', '/resources/')


    # --------------------------------------------------------------------------
    #
    def find_resources(self, states=None):

        if   not states                  : states = list()
        elif not isinstance(states, list): states = [states]

        ret  = list()
        rids = self.list_resources()

        states = self.get_resource_states(rids)
        for rid,state in zip(rids, states):
            if state in states:
                ret.append(rid)

        return ret


    # --------------------------------------------------------------------------
    #
    def get_resource_info(self, resource_ids=None):

        if not resource_ids:
            resource_ids = self.list_resources()
        elif not isinstance(resource_ids, list): 
            resource_ids = [resource_ids]

        ret = list()
        for rid in resource_ids:

            info = self._query('get', '/resources/%s/info' % rid)
            ret.append(info)

        return ret


    # --------------------------------------------------------------------------
    #
    def get_requested_resources(self):

        return self._query('get', '/resources/requested')


    # --------------------------------------------------------------------------
    #
    def get_available_resources(self):

        return self._query('get', '/resources/available')


    # --------------------------------------------------------------------------
    #
    def get_resource_states(self, resource_ids=None):

        if not resource_ids:
            resource_ids = self.list_resources()
        elif not isinstance(resource_ids, list):
            resource_ids = [resource_ids]

        ret = list()
        for rid in resource_ids:

            state = self._query('get', '/resources/%s/state' % rid)
            ret.append(state)

        return ret


    # --------------------------------------------------------------------------
    #
    def wait_resource_states(self, resource_ids=None, 
                             states=None, timeout=None):

        if not isinstance(states, list): states = [states]
        else:
            pass
          # raise NotImplementedError('can only wait for one state')

        state = states[0]

        # FIXME: this is state model agnostic - passed states will never be
        #        matched
        if not resource_ids:
            resource_ids = self.list_resources()
        elif not isinstance(resource_ids, list):
            resource_ids = [resource_ids]

        for rid in resource_ids:

            self._query('get', '/resources/%s/wait/%s/%s' % (rid, state, timeout))

        return


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, descriptions):

        if   not descriptions                  : descriptions = list()
        elif not isinstance(descriptions, list): descriptions = [descriptions]

        return self._query('put', '/tasks/', data=descriptions)


    # --------------------------------------------------------------------------
    #
    def list_tasks(self):

        return self._query('get', '/tasks/')


    # --------------------------------------------------------------------------
    #
    def get_task_states(self, task_ids=None):

        if not task_ids:
            task_ids = self.list_tasks()
        elif not isinstance(task_ids, list):
            task_ids = [task_ids]

        ret = list()
        for tid in task_ids:

            state = self._query('get', '/tasks/%s/state' % tid)
            ret.append(state)

        return ret


    # --------------------------------------------------------------------------
    #
    def wait_task_states(self, task_ids=None, states=None, timeout=None):

        if not isinstance(states, list): states = [states]
        else:
            pass
          # raise NotImplementedError('can only wait for one state')

        state = states[0]

        # FIXME: this is state model agnostic - passed states will never be
        #        matched
        if not task_ids:
            task_ids = self.list_tasks()
        elif not isinstance(task_ids, list):
            task_ids = [task_ids]

        for rid in task_ids:
            self._query('get', '/tasks/%s/wait/%s/%s' % (rid, state, timeout))

        return


# ------------------------------------------------------------------------------

