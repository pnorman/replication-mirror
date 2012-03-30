#!/usr/bin/env python

import sys
import urllib2
from datetime import datetime
import time
import os
import errno

import config

VERBOSE = False

def isoToTimestamp(isotime):
  t = datetime.strptime(isotime, "%Y-%m-%dT%H:%M:%SZ")
  return time.mktime(t.timetuple())


def minutelyUpdateRun():

    # Read the state.txt
    
    state = {}
    with open(os.path.realpath(os.path.dirname(sys.argv[0])) + '/state.txt', 'r') as sf:

        for line in sf:
            if line[0] == '#':
                continue
            (k, v) = line.split('=')
            state[k] = v.strip().replace("\\:", ":")

    minuteNumber = int(isoToTimestamp(state['timestamp'])) / 60
    

    # Grab the sequence number and build a URL out of it
    sqnStr = str(int(state['sequenceNumber'])+1).zfill(9)
    url = config.REPLICATE_BASE + '%s/%s/%s.osc.gz' % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])

    try:
        os.makedirs(os.path.realpath(os.path.dirname(sys.argv[0])) + '/{}/{}'.format(sqnStr[0:3], sqnStr[3:6]))
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
            
    if VERBOSE:
        print "Downloading change file (%s)." % (url)
        
    try:
        uosc = urllib2.urlopen(url)
        with open(os.path.realpath(os.path.dirname(sys.argv[0])) + '/{}/{}/{}.osc.gz'.format(sqnStr[0:3], sqnStr[3:6], sqnStr[6:9]), 'w') as losc:
            losc.write(uosc.read())
    except urllib2.HTTPError, e:
        if e.code == 404:
          return False
        raise e
        
    url = config.REPLICATE_BASE + '%s/%s/%s.state.txt' % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])
    
    try:
        ustate = urllib2.urlopen(url)
        sstate = ustate.read()
        with open(os.path.realpath(os.path.dirname(sys.argv[0])) + '/{}/{}/{}.state.txt'.format(sqnStr[0:3], sqnStr[3:6], sqnStr[6:9]), 'w') as lstate:
            lstate.write(sstate)
        with open(os.path.realpath(os.path.dirname(sys.argv[0])) + '/state.txt'.format(sqnStr[0:3], sqnStr[3:6], sqnStr[6:9]), 'w') as lstate:
            lstate.write(sstate)
    except urllib2.HTTPError, e:
        if e.code == 404:
          return False
        raise e

    return True

    

if __name__ == "__main__":
    if os.path.isfile(os.path.realpath(os.path.dirname(sys.argv[0])) + '/download.lock'):
        print 'Lockfile found'
    else:
        try:
            with open(os.path.realpath(os.path.dirname(sys.argv[0])) + '/download.lock', 'w') as lockfile:
                while minutelyUpdateRun():
                    pass
        finally:
            os.remove(os.path.realpath(os.path.dirname(sys.argv[0])) + '/download.lock')