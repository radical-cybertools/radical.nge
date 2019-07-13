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

    sid   = 'foo.1'
    url   = 'http://guest:guest@login1:8090/'

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


# ------------------------------------------------------------------------------

