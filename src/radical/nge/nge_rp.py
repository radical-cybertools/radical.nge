
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.pilot as rp
import radical.utils as ru

from .utils import get_backfill


MAX_CORES    = 160  # 10 nodes on titan
MAX_WALLTIME =  60  #  1 hour == debug on titan


# ------------------------------------------------------------------------------
#
def tolist(thing):

    if   thing is None          : return []
    elif isinstance(thing, list): return thing
    else                        : return [thing]


# --------------------------------------------------------------------------
#
class NGE_RP(object):
    '''
    This class interfaces from a NGE_RS like API to radical.pilot
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, log=None, rep=None, prof=None):

        if log : self._log  = log
        else   : self._log  = ru.Logger('radical.nge')

        if rep : self._rep  = log
        else   : self._rep  = ru.Reporter('radical.nge')

        if prof: self._prof = prof
        else   : self._prof = ru.Profiler('radical.nge')

        self._session = rp.Session()
        self._pmgr    = rp.PilotManager(self._session)
        self._umgr    = rp.UnitManager(self._session)

        self._pmgr.register_callback(self._pilot_state_cb)
        self._umgr.register_callback(self._unit_state_cb)

        # create a dir for data staging
        self._pwd  = os.getcwd()
        self._data = 'data.%s' % self._session.uid
        os.makedirs('%s/%s/'   % (self._pwd, self._data))


        # track submitted tasks
        self._tcnt  = 0
        self._tasks = dict()


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):

        return self._session.uid


    # --------------------------------------------------------------------------
    #
    def close(self):

        self._session.close(download=True)


    # --------------------------------------------------------------------------
    #
    def pilots_submit(self, requests):

        req_norm = list()
        req_bf   = list()

        for request in requests:

            bf = bool(request.get('backfill'))

            if bf: req_bf.append(request)
            else : req_norm.append(request)

        pds = list()
        if req_bf:
            pds += self._pilots_backfill(req_bf)

        if req_norm:
            pds += self._pilots_queue(req_norm)

        pilots = self._pmgr.submit_pilots(pds)
        self._umgr.add_pilots(pilots)

        return [p.uid for p in pilots]


    # --------------------------------------------------------------------------
    #
    def _pilots_backfill(self, requests):
        '''
        Request new backfill pilots, chunked by the given max_cores and
        max_walltime.  The given request_stub is used as template for the pilot
        descriptions.
        '''

        self._rep.info('\nrequesting backfilled pilots\n')
        pds = list()

        for request in requests:

            del(request['backfill'])

            policy       = request['policy']
            partition    = request['partition']

            PWD          = os.path.dirname(__file__)
            policy       = ru.read_json('%s/policies/%s.json'
                                       % (PWD, request['policy']))

            max_cores    = policy.get('max_cores'   , MAX_CORES   )
            max_walltime = policy.get('max_walltime', MAX_WALLTIME)

            self._rep.info('\nrequesting backfill pilots\n')
            bf = get_backfill(request['partition'], max_cores, max_walltime)

            for [partition, cores, walltime] in bf:
                pd = {'resource': request.get('resource', 'local.localhost'),
                      'project' : request.get('project'),
                      'queue'   : request.get('queue'),
                      'cores'   : cores, 
                      'runtime' : walltime
                     }
                self._rep.ok('backfill @ %s [%5dcores * %4dmin] @ %10s(%10s)]\n'
                            % (pd['resource'], pd['cores'], pd['runtime'],
                               pd['queue'],    pd['project']))
              # pprint.pprint(pd)
                pds.append(rp.ComputePilotDescription(pd))

        return pds


    # --------------------------------------------------------------------------
    #
    def _pilots_queue(self, requests):
        '''
        submit a new pilot to the batchs system
        '''

        self._rep.info('\nrequesting dedicated pilots\n')
        pds = list()

        for request in requests:
            pd  = {'resource' : request.get('resource', 'local.localhost'),
                    'project' : request.get('project'),
                    'queue'   : request.get('queue'),
                    'cores'   : request['cores'],
                    'runtime' : request['walltime']
                  }
            self._rep.ok('provision on %s [%5dcores * %4dmin] @ %10s(%10s)]\n' %
                         (pd['resource'], pd['cores'], pd['runtime'],
                          pd['queue'], pd['project']))
            pds.append(rp.ComputePilotDescription(pd))

        return pds


    # --------------------------------------------------------------------------
    #
    def pilots_inspect(self, pids=None):

        self._rep.info('\nget pilot info: %s\n' % pids)

        if pids and not isinstance(pids, list): pids = pids

        pinfo  = list()
        pilots = self._pmgr.get_pilots(uids=pids)

        if   not pilots                  : pilots = list()
        elif not isinstance(pilots, list): pilots = [pilots]

        for pilot in pilots:
            self._rep.ok('    %s\n' % pilot.uid)
            pinfo.append(pilot.as_dict())

        return pinfo


    # --------------------------------------------------------------------------
    #
    def pilots_wait(self, pids=None, states=None, timeout=None):

        pids = tolist(pids)

        self._rep.info('\nwait for pilots: %s (%s) (%s)\n'
                      % (pids, states, timeout))
        return self._pmgr.wait_pilots(uids=pids, state=states, timeout=timeout)


    # --------------------------------------------------------------------------
    #
    def pilots_cancel(self, pids=None):

        self._rep.info('\ncancel pilots %s\n' % pids)

        self._pmgr.cancel_pilots(pids)
        self._pmgr.wait_pilots  (pids, rp.FINAL)

        if pids and not isinstance(pids, list): pids = pids

        states = list()
        for pilot in self._pmgr.get_pilots(pids):
            self._rep.ok('%s: %10s\n' % (pilot.uid, pilot.state))
            states.append(pilot.state)

        return states


    # --------------------------------------------------------------------------
    #
    def tasks_submit(self, descriptions):

        # FIXME: we actually get PANDA task descriptions here, which we need to
        #        translate into RP unit descriptions

        # before we hand over tasks to the RP layer, we will stage files
        # FIXME: panda level input file staging goes here.

        self._rep.header('submit tasks\n')
        cuds = list()
        orig = self._tcnt
        for descr in descriptions:
            # ensure we stage stdout/stderr.  We don't have unit ID's yet, so we
            # use a custom task ID to identify the output files.  Later versions
            # of RP will allow :${RP_UNIT_ID}.
            tid = 'task.%06d' % self._tcnt
            self._tcnt += 1

            tmp = [{'source': 'unit:///STDOUT',
                    'target': 'client:///%s/%s.out' % (self._data, tid),
                    'action': rp.TRANSFER},
                   {'source': 'unit:///STDERR',
                    'target': 'client:///%s/%s.err' % (self._data, tid),
                    'action': rp.TRANSFER}]

            if 'output_staging' in descr:
                descr['output_staging'] += tmp
            else:
                descr['output_staging']  = tmp

            cuds.append(rp.ComputeUnitDescription(descr))

        units = self._umgr.submit_units(cuds)

        self._tcnt = orig
        for unit in units:
            tid = 'task.%06d' % self._tcnt
            self._tcnt += 1
            self._tasks[unit.uid] = tid

        return [unit.uid for unit in units]


    # --------------------------------------------------------------------------
    #
    def _pilot_state_cb(self, pilot, state):

        if state in rp.FINAL:
            self._rep.ok('pilot completed %s: %s\n' % (pilot.uid, pilot.state))
            self._umgr.remove_pilots(pilot.uid)

        return True


    # --------------------------------------------------------------------------
    #
    def _unit_state_cb(self, unit, state):

        # once the units are in final state, we can run the panda output staging
        # routines.  To learn about final units, we registered this unit state
        # callback.on umgr creation.
        if state == rp.DONE:
            self._rep.ok('task completed %s\n' % unit.uid)
            pass
            # FIXME: panda notification
        elif state == rp.FAILED:
            self._rep.error('task failed    %s\n' % unit.uid)
            pass

        return True


    # --------------------------------------------------------------------------
    #
    def tasks_inspect(self, tids=None):

        self._rep.info('\nget task info: %s\n' % tids)

        if tids and not isinstance(tids, list): tids = tids

        tinfo = list()
        tasks = self._umgr.get_units(uids=tids)

        if   not tasks                  : tasks = list()
        elif not isinstance(tasks, list): tasks = [tasks]

        for task in tasks:
            self._rep.ok('    %s\n' % task.uid)
            tinfo.append(task.as_dict())

        return tinfo


    # --------------------------------------------------------------------------
    #
    def tasks_stdout(self, tid):

        self._rep.info('\nget task stdout: %s\n' % tid)

        if tid not in self._tasks:
            raise ValueError('task ID is unknown')

        fname = '%s/%s/%s.out' % (self._pwd, self._data, self._tasks[tid])

        if not os.path.isfile(fname):
            raise RuntimeError('stdout for %s is not available' % tid)

        return open(fname, 'r').read()


    # --------------------------------------------------------------------------
    #
    def tasks_stderr(self, tid):

        self._rep.info('\nget task stderr: %s\n' % tid)

        if tid not in self._tasks:
            raise ValueError('task ID is unknown')

        fname = '%s/%s/%s.err' % (self._pwd, self._data, self._tasks[tid])

        if not os.path.isfile(fname):
            raise RuntimeError('stderr for %s is not available' % tid)

        return open(fname, 'r').read()


    # --------------------------------------------------------------------------
    #
    def tasks_wait(self, tids=None, states=None, timeout=None):

        if tids and not isinstance(tids, list): tids = tids

        self._rep.info('\nwait for tasks: %s (%s)\n' % (tids, states))
        return self._umgr.wait_units(uids=tids, state=states, timeout=timeout)


# ------------------------------------------------------------------------------

