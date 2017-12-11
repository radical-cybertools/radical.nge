#!/usr/bin/env python

import radical.pilot as rp
import radical.nge   as rn

import pprint

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    nge = None
    try:
      # nge = rn.NGE(binding=rn.RP,  url=None)
        nge = rn.NGE(binding=rn.RPS, url='http://localhost:8090/')

        print nge.login (username='guest', password='guest')
        print 'session id: %s' % nge.uid

        print 'request backfill resources'
        pprint.pprint(nge.request_backfill_resources(
                                           {'resource' : 'ornl.titan_aprun', 
                                            'queue'    : 'debug',
                                            'project'  : "CSC230"},
                                           partition='titan',
                                           policy='default'))
        print

        print 'inspect resources'
        print nge.list_resources()
        print

        print 'request resources'
        print nge.request_resources([{'resource' : 'ornl.titan_orte', 
                                            'queue'    : 'debug',
                                            'cores'    : 160,
                                            'project'  : "CSC230",
                                            'walltime' : 20}])
        print

        print 'inspect resources'
        print nge.list_resources()
        print

        print 'find resources'
        print nge.find_resources()
        print

        print 'get_requested_resources'
        print nge.get_requested_resources()
        print

        print 'get_available_resources'
        print nge.get_available_resources()
        print

        print 'get_resource_info'
        print nge.get_resource_info()
        print

        print 'get_resource_states'
        print nge.get_resource_states()
        print

        print 'wait_resource_states'
        print nge.wait_resource_states(states=rp.PMGR_ACTIVE)
        print

        print 'submit a task'
        print nge.submit_tasks([{'executable' : '/bin/true'}])
        print

        print 'list tasks'
        print nge.list_tasks()
        print

        print 'get task states'
        print nge.get_task_states()
        print

        print 'wait for task completion'
        print nge.wait_task_states(states=rp.FINAL)
        print

    finally:
        if nge:
            print 'close panda-nge session'
            nge.close()
            print



#-------------------------------------------------------------------------------

