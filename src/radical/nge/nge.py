
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.pilot as rp

RP   = 'radical.pilot'
RPS  = 'radical.pilot.service'
DB   = 'radical.pilot.db'

# --------------------------------------------------------------------------
#
# see https://docs.google.com/document/d/1bm8ucgfi9SHjDy0w-ZX5NIdkjk87qFClMB9jMse75uM
#
class NGE(object):
    '''
    This is an abstract base class for the Panda-NGE integration API.  We will
    provide different implementations to use it:

      - REST as the outward facing API of the server
      - dirctly over RP, for use in the server implementation

    The API binding is determined at session construction, and default to RP.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, binding=RP, url=None, reporter=None):
        '''
        binding: API implementation binding [RP, RPS]
        url    : contact point for the service and DB bindings
        '''

        self._rep = reporter

        from .nge_rp  import NGE_RP
        from .nge_rps import NGE_RPS

        if   binding == RP : self._binding = NGE_RP (url, reporter)
        elif binding == RPS: self._binding = NGE_RPS(url, reporter)
     #  elif binding == DB : self._binding = NGE_DB (url, reporter)
        else               : raise NotImplementedError('unknown binding %s' % binding)


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        '''
        return the server UID
        '''

        return self._binding.uid


    # --------------------------------------------------------------------------
    #
    def login(self, username, password):

        return self._binding.login(username, password)


    # --------------------------------------------------------------------------
    #
    def close(self):
        '''
        free all aquired and requested resources, cancel all non-final tasks.
        '''

        return self._binding.close()


    # --------------------------------------------------------------------------
    #
    def request_backfill_resources(self, request_stub, partition=None,
                                         policy=None):
        '''
        request resources as backfill jobs.
        '''

        return self._binding.request_backfill_resources(request_stub, partition,
                                                        policy)


    # --------------------------------------------------------------------------
    #
    def request_resources(self, requests):
        '''
        request a new resource (ie. submit a new RP pilot) for a given set of
        cores / walltime.
        '''

        return self._binding.request_resources(requests)


    # --------------------------------------------------------------------------
    #
    def list_resources(self):
        '''
        return the UIDs for all known resources (ie. RP pilots), independent of
        their state.
        '''

        return self._binding.list_resources()


    # --------------------------------------------------------------------------
    #
    def find_resources(self, states=None):
        '''
        return the UIDs for all known resources (ie. RP pilots) in the given
        states, or in any state if no state filter is defined.
        '''

        # FIXME: this can be c ombined with `list_resources()`

        return self._binding.find_resources(states)


    # --------------------------------------------------------------------------
    #
    def get_requested_resources(self):
        '''
        find all resources (ie. pilots) in *any* state, and return number of
        cores, walltime and state as tuples.
        '''
        # FIXME: this can be combined with
        #        `find_resources()` / `get_resource_info()`

        return self._binding.get_requested_resources()


    # --------------------------------------------------------------------------
    #
    def get_available_resources(self):
        '''
        find all `ACTIVE` resources (ie. pilots) and return number of cores,
        walltime and state as tuples.
        '''
        # FIXME: this can be combined with
        #        `find_resources()` / `get_resource_info()`

        return self._binding.get_available_resources()


    # --------------------------------------------------------------------------
    #
    def get_resource_info(self, resource_ids=None):
        '''
        get information for all resources (ie. RP pilots) with the given UIDs
        (or for all known resources if no UID is specified).
        '''

        return self._binding.get_resource_info(resource_ids)


    # --------------------------------------------------------------------------
    #
    def get_resource_states(self, resource_ids=None):
        '''
        get the state for all resources (ie. RP pilots) with the given UIDs
        (or for all known resources if no UID is specified).
        '''

        return self._binding.get_resource_states(resource_ids)

    # --------------------------------------------------------------------------
    #
    def wait_resource_states(self, resource_ids=None, states=None, timeout=None):
        '''
        wait for a specific (set of) states for all resources (ie. RP pilots)
        with the given UIDs (or for all known resources if no UID is specified).
        This call will return after a given timeout, or after the states have
        been reached, whichever occurs first.  A negative timeout value will
        cause it to wait forever.
        '''

        return self._binding.wait_resource_states(resource_ids, states, timeout)


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, descriptions):
        '''
        panda task descriptions are submitted to the RP level resources
        (pilots).
        '''
        # FIXME: do we need direct binding?

        return self._binding.submit_tasks(descriptions)


    # --------------------------------------------------------------------------
    #
    def list_tasks(self):
        '''
        return UIDs for all known tasks (ie. RP units)
        '''

        return self._binding.list_tasks()


    # --------------------------------------------------------------------------
    #
    def get_task_states(self, task_ids=None):
        '''
        return states for the tasks (ie. RP units) with the given UIDs, or for
        all tasks, if no UIDs are specified
        '''

        return self._binding.get_task_states(task_ids)


    # --------------------------------------------------------------------------
    #
    def wait_task_states(self, task_ids=None, states=None, timeout=None):
        '''
        wait for a specific (set of) states for all tasks (ie. RP units)
        with the given UIDs (or for all known tasks if no UID is specified).
        This call will return after a given timeout, or after the states have
        been reached, whichever occurs first.  A negative timeout value will
        cause it to wait forever.
        '''

        return self._binding.wait_task_states(task_ids, states, timeout)


# ------------------------------------------------------------------------------

