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

    sid   = 'foo.1'
    url   = 'http://guest:guest@localhost:8090/'

    nge = rn.NGE_RS(url=url)

    print 'inspect sessions'
    print nge.sessions_inspect()

    print 'login'
    nge = rn.NGE_RS(url=url)

    print 'inspect sessions'
    print nge.sessions_inspect()

    print 'create session'
    nge.sessions_create(sid)
    print 'sid: %s' % sid

    print 'inspect sessions'
    print nge.sessions_inspect()

  # nge.pilots_submit(sid, 
  #                   [{'type'     : 'backfill', 
  #                     'resource' : 'ornl.titan_aprun',
  #                     'queue'    : 'debug',
  #                     'project'  : "CSC230", 
  #                     'partition': titan',
  #                     'policy'   : 'default'}])
  #
    print 'submit pilots (normal)'
    if tgt == 'titan':
        nge.pilots_submit(sid, 
                          [{'resource' : 'ornl.titan_aprun',
                            'queue'    : 'debug',
                            'project'  : 'BIP149',
                            'cores'    : psize + 16 * 1,
                            'walltime' : 60}])
    else:
        nge.pilots_submit(sid, 
                          [{'resource' : 'local.localhost',
                            'cores'    : 160,
                            'walltime' : 20}])

    print 'inspect pilots'
    info = nge.pilots_inspect(sid)
    for p in info:
        print '%s: %s' % (p['uid'], p['state'])


# ------------------------------------------------------------------------------

