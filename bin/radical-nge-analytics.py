#!/usr/bin/env python

import sys

import radical.utils     as ru
import radical.analytics as ra

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    if len(sys.argv) != 2:
        print "\n\tusage: %s <session dir|tarball>\n" % sys.argv[0]
        sys.exit(1)

    session = ra.Session(sys.argv[1], 'radical.pilot')
    pilots  = session.get(etype='pilot')

    for pilot in pilots:
        states = pilot.states
        for state, event in sorted(pilot.states.items(),
                                   key=lambda x: x[1][ru.TIME]):
            print '%-25s : %5.2f' % (state, event[ru.TIME])


# ------------------------------------------------------------------------------

