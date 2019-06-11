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
                            'walltime' : 20}])
    else:
        nge.pilots_submit(sid, 
                          [{'resource' : 'local.localhost',
                            'cores'    : 160,
                            'walltime' : 3}])

    print 'inspect pilots'
    info = nge.pilots_inspect(sid)
    for p in info:
        print '%s: %s' % (p['uid'], p['state'])


    print 'wait pilots'
    print nge.pilots_wait(sid, states=rp.PMGR_ACTIVE)
    print 'ok'

    print 'inspect pilots'
    info = nge.pilots_inspect(sid)
    for p in info:
        print '%s: %s' % (p['uid'], p['state'])
    print 'ok'


# ------------------------------------------------------------------------------

