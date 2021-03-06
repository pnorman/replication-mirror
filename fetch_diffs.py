#!/usr/bin/env python

import sys
import urllib2
from datetime import datetime
import time
import os
import errno
import gzip
import config
import cStringIO as StringIO
from lxml import etree

VERBOSE = True

    
def isoToTimestamp(isotime):
  t = datetime.strptime(isotime, "%Y-%m-%dT%H:%M:%SZ")
  return time.mktime(t.timetuple())


def minutelyUpdateRun():

    # Read the state.txt
    
    state = {}
    with open(config.REPLICATE_DISK + '/state.txt', 'r') as sf:

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
        os.makedirs(config.REPLICATE_DISK + '/{}/{}'.format(sqnStr[0:3], sqnStr[3:6]))
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
            
    if VERBOSE:
        print "Downloading change file (%s)." % (url)
        
    try:
        # Read the upstream .osc.gz
        uosc = StringIO.StringIO(urllib2.urlopen(url).read())

        # Try to decompress it, parse it and check the root tag. If any of these fail, bail out with an exception
        if etree.parse(StringIO.StringIO(gzip.GzipFile(fileobj=uosc).read())).getroot().tag != 'osmChange':
            raise lxml.etree.ParseError

        #reset the file for later
        uosc.seek(0)
            
        with open(config.REPLICATE_DISK + '/{}/{}/{}.osc.gz'.format(sqnStr[0:3], sqnStr[3:6], sqnStr[6:9]), 'w') as losc:
            losc.write(uosc.read())
    except urllib2.HTTPError, e:
        if e.code == 404:
          return False
        raise e
        
    url = config.REPLICATE_BASE + '%s/%s/%s.state.txt' % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])

    if VERBOSE:
        print "Downloading state file (%s)." % (url) 
    try:
        ustate = urllib2.urlopen(url)
        sstate = ustate.read()
        with open(config.REPLICATE_DISK + '/{}/{}/{}.state.txt'.format(sqnStr[0:3], sqnStr[3:6], sqnStr[6:9]), 'w') as lstate:
            lstate.write(sstate)
        with open(config.REPLICATE_DISK + '/state.txt'.format(sqnStr[0:3], sqnStr[3:6], sqnStr[6:9]), 'w') as lstate:
            lstate.write(sstate)
    except urllib2.HTTPError, e:
        if e.code == 404:
          return False
        raise e

    return True

    

if __name__ == "__main__":
    if os.path.isfile(config.REPLICATE_DISK + '/download.lock'):
        print 'Lockfile found'
    else:
        try:
            with open(config.REPLICATE_DISK + '/download.lock', 'w') as lockfile:
                while minutelyUpdateRun():
                    pass
        finally:
            os.remove(config.REPLICATE_DISK + '/download.lock')