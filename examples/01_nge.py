#!/usr/bin/env python

import sys
import time

import radical.pilot as rp
import radical.nge   as rn

tgt = 'titan'
tgt = 'local'

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    if len(sys.argv) > 1:
        n = int(sys.argv[1])
    else:
        n = 32

    unum  = n
    ugen  = 2    # generations
    usize = 4    # threads
    utime = 1    # sec
    psize = int(n * usize / ugen)

    nge = None
    sid = 'foo.1'
    url = 'http://guest:guest@localhost:8090/'

    print 'connect'
    nge = rn.NGE_RS(url=url)

    print 'inspect sessions'
    info = nge.pilots_inspect(sid)
    for p in info:
        print '%s: %s' % (p['uid'], p['state'])

    print 'inspect pilots'
    info = nge.pilots_inspect(sid)
    for p in info:
        print '%s: %s' % (p['uid'], p['state'])
    print 'ok'

    print 'wait pilots'
    print nge.pilots_wait(sid, states=rp.PMGR_ACTIVE)
    print 'ok'

    print 'inspect pilots'
    info = nge.pilots_inspect(sid)
    for p in info:
        print '%s: %s' % (p['uid'], p['state'])
    print 'ok'

    print 'submit tasks'
    tasks = list()
    for _ in range(unum):
        tasks.append({'executable'       : '/bin/sleep',
                      'arguments'        : [utime],
                      'cpu_processes'    : 1,
                      'cpu_threads'      : usize,
                      'cpu_process_type' : rp.POSIX,
                      'cpu_thread_type'  : rp.POSIX,
                      'pilot'            : None
                    })
    print nge.tasks_submit(sid, tasks)
    print 'ok'

    print 'inspect tasks'
    info = nge.tasks_inspect(sid)
    for t in info:
        print '%s: %s' % (t['uid'], t['state'])
    print 'ok'

    print 'wait for task completion'
    print nge.tasks_wait(sid, states=rp.FINAL)
    print 'ok'

    print 'inspect tasks'
    info = nge.tasks_inspect(sid)
    for t in info:
        print '%s: %s' % (t['uid'], t['state'])
    print 'ok'

    print 'cancel resources'
    print nge.pilots_cancel(sid)
    print 'ok'

    print 'inspect pilots'
    info = nge.pilots_inspect(sid)
    for p in info:
        print '%s: %s' % (p['uid'], p['state'])
    print 'ok'


# ------------------------------------------------------------------------------

