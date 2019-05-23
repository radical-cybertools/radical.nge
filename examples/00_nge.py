#!/usr/bin/env python

import sys
import time

import radical.pilot as rp
import radical.nge   as rn

t0 = time.time()
ts = list()

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    psize = 32  # 16   * 64
    unum  = 32  # 1024 * 4
    cores = 4
    utime = 1     # min

    ts.append('%10s: %8d' % ('psize', psize))
    ts.append('%10s: %8d' % ('unum ', unum ))
    ts.append('%10s: %8d' % ('cores', cores))
    ts.append('%10s: %8d' % ('utime', utime))


    def mark(m):
        global t0
        t = time.time()
        diff = t - t0
        out  = '%10s: %6.2f' % (m, diff)
        ts.append(out)
        print out
        t0 = t

    nge   = None
    n     = 100
    if len(sys.argv) > 1:
        unum = int(sys.argv[1])

    try:
      # nge = rn.NGE(binding=rn.RP,  url=None)
        nge = rn.NGE(binding=rn.RPS, url='http://localhost:8090/')
      # nge = rn.NGE(binding=rn.RPS, url='http://two.radical-project.org:8080/')

        nge.restart()
        mark('s restart')

        nge.login(username='guest', password='guest')
        mark('s login')
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
        print nge.request_resources([{'resource' : 'ornl.titan_aprun',
                                      'queue'    : 'debug',
                                      'project'  : 'BIP149',
                                      'cores'    : psize + 16*1,  # add an agent node
                                      'walltime' : 60}])
      # print nge.request_resources([{'resource' : 'local.localhost',
      #                               'cores'    : 160,
      #                               'walltime' : 20}])
        print 'ok'
        mark('p submit')

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
        mark('p wait')

        print 'submit a task'
        tasks = list()
        for _ in range(unum):
            tasks.append({  # 'executable'       : '/bin/true',
                              'executable'       : '/bin/sh',
                              'arguments'        : ['gromacs_canon/gromacs.sh', cores, utime],
                              'input_staging'    : [{'source': 'file:///ccs/home/merzky1/radical/radical.nge/examples/gromacs.canon', 
                                                     'target': 'unit:///gromacs_canon',
                                                     'action': rp.TARBALL}],
                              'cpu_processes'    : 1,
                              'cpu_threads'      : cores,
                              'cpu_process_type' : rp.POSIX,
                              'cpu_thread_type'  : rp.POSIX,
                              'cpu_process_type' : 'fork',
                        })
        print nge.submit_tasks(tasks)
        print 'ok'
        mark('u submit')

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
        mark('u wait')

        print 'get task states'
        print nge.get_task_states()
        print 'ok'
        mark('u get')

        print 'cancel resources'
        print nge.cancel_resources()
        print 'ok'
        mark('p cancel')


    finally:
        if nge:
         #  print 'close panda-nge session'
         #  nge.close()
            print 'ok'

    mark('s close')
    ts.append('')
    with open('nge.dat', 'a+') as fout:
        for t in ts:
            print t
            fout.write(t)

# ------------------------------------------------------------------------------

