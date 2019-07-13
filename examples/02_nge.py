#!/usr/bin/env python

import sys

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
        n = 5

    unum  = n
    ugen  = 2    # generations
    usize = 4    # threads
    utime = 1    # sec

    nge = None
    sid = 'foo.1'
    url = 'http://guest:guest@login1:8090/'

    print 'connect'
    nge = rn.NGE_RS(url=url)

    print 'inspect sessions'
    info = nge.pilots_inspect(sid)
    for p in info:
        print '%s: %s' % (p['uid'], p['state'])

    print 'submit tasks'
    tasks = list()
    for _ in range(unum):
        tasks.append({'executable'       : '/bin/date',
                    # 'arguments'        : [utime],
                      'cpu_processes'    : 1,
                      'cpu_threads'      : usize,
                      'cpu_process_type' : rp.POSIX,
                      'cpu_thread_type'  : rp.POSIX,
                      'pilot'            : None,
                    })
    tids = nge.tasks_submit(sid, tasks)
    print tids
    print 'ok'

    print 'inspect tasks'
    info = nge.tasks_inspect(sid)
    for t in info:
        print '%s: %s [%s]' % (t['uid'], t['state'], t['stdout'])
    print 'ok'

    print 'wait for task completion'
    print nge.tasks_wait(sid, states=rp.FINAL)
    print 'ok'

    print 'inspect tasks'
    info = nge.tasks_inspect(sid)
    for t in info:
        print '%s: %s: %s' % (t['uid'], t['state'], t['stdout'])
    print 'ok'

    print 'stdout for %s' % tids[0]
    print nge.tasks_stdout(sid, tids[0])

    try:
        print 'stderr for %s' % tids[0]
        print nge.tasks_stderr(sid, tids[0])
    except Exception as e:
        print e


# ------------------------------------------------------------------------------

