#!/usr/bin/env python

import sys
import time

import radical.pilot as rp
import radical.nge   as rn


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    times = list()
    nge   = None
    n     = 1
    if len(sys.argv) > 1:
        n = int(sys.argv[1])

    try:
      # nge = rn.NGE(binding=rn.RP,  url=None)
        nge = rn.NGE(binding=rn.RPS, url='http://localhost:8090/')
      # nge = rn.NGE(binding=rn.RPS, url='http://two.radical-project.org:8080/')

        times.append(time.time())
        nge.restart()
        times.append(time.time())

        print nge.login (username='guest', password='guest')
        times.append(time.time())
        print 'session id: %s' % nge.uid

      # print 'request backfill resources'
      # pprint.pprint(nge.request_backfill_resources(
      #                                    {'resource' : 'ornl.titan_aprun',
      #                                     'queue'    : 'debug',
      #                                     'project'  : "CSC230"},
      #                                    partition='titan',
      #                                    policy='default'))
      # print 'ok'
      #
      # print 'inspect resources'
      # print nge.list_resources()
      # print 'ok'

        print 'request resources'
        print nge.request_resources([{'resource' : 'ornl.titan_orte',
                                            'queue'    : 'debug',
                                            'project'  : 'BIP149',
                                            'cores'    : 160,
                                            'walltime' : 20}])
      # print nge.request_resources([{'resource' : 'local.localhost',
      #                                     'cores'    : 160,
      #                                     'walltime' : 20}])
        print 'ok'
        times.append(time.time())

#       print 'inspect resources'
#       print nge.list_resources()
#       print 'ok'
#
#       print 'find resources'
#       print nge.find_resources()
#       print 'ok'
#
#       print 'get_requested_resources'
#       print nge.get_requested_resources()
#       print 'ok'
#
#       print 'get_available_resources'
#       print nge.get_available_resources()
#       print 'ok'
#
#       print 'get_resource_info'
#       print nge.get_resource_info()
#       print 'ok'
#
#       print 'get_resource_states'
#       print nge.get_resource_states()
#       print 'ok'
 
        print 'wait_resource_states'
        print nge.wait_resource_states(states=rp.PMGR_ACTIVE)
        print 'ok'
        times.append(time.time())

        print 'submit a task'
        tasks = list()
        for _ in range(n):
            tasks.append({  # 'executable'       : '/bin/true',
                              'executable'       : '/bin/sh',
                              'arguments'        : 'gromacs/gromacs.sh',
                              'input_staging'    : [{'source': 'client:///gromacs', 
                                                     'target': 'unit:///gromacs',
                                                     'action': 'TARBALL'}],
                              'cpu_processes'    : 1,
                              'cpu_process_type' : 'fork'})
                        })
        print nge.submit_tasks(tasks)
        print 'ok'
        times.append(time.time())

#       print 'list tasks'
#       print nge.list_tasks()
#       print 'ok'
#
#       print 'get task states'
#       print nge.get_task_states()
#       print 'ok'

        print 'wait for task completion'
        print nge.wait_task_states(states=rp.FINAL)
        print 'ok'
        times.append(time.time())

        print 'get task states'
        print nge.get_task_states()
        print 'ok'

        print 'cancel resources'
        print nge.cancel_resources()
        print 'ok'
        times.append(time.time())


    finally:
        if nge:
            print 'close panda-nge session'
            nge.close()
            print 'ok'

    times.append(time.time())
    data = '%6d \t %s\n' % (n, ''.join(['\t%6.2f' % (t - times[0]) 
                                        for t in times]))
    with open('nge.dat', 'a+') as fout:
        print data
        fout.write(data)

# ------------------------------------------------------------------------------

