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

    psize = 160

    sid = 'foo.1'
  # url = 'http://guest:guest@login1:8090/'
    url = 'http://guest:guest@localhost:8090/'

    print 'connect'
    nge = rn.NGE_RS(url=url)

    print 'inspect sessions'
    info = nge.sessions_inspect()
    for _sid in info:
        print '  %-10s: %s' % (_sid, info['uid'])

    print 'login'
    nge = rn.NGE_RS(url=url)

    print 'inspect sessions'
    info = nge.sessions_inspect()
    for _sid in info:
        print '  %-10s: %s' % (_sid, info['uid'])

    print 'create session'
    nge.sessions_create(sid)
    print 'sid: %s' % sid

    print 'inspect sessions'
    info = nge.sessions_inspect()
    for _sid in info:
        print '  %-10s: %s' % (_sid, info[sid])

    print 'inspect sessions'
    print nge.sessions_inspect()


# ------------------------------------------------------------------------------

