#!/usr/bin/env python
"""mapper.py"""

import sys
import re

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = re.split(r'[,\s]\s*', line)
    # increase counters
    time= re.split(r'[-,\s]\s*',words[0])

    try:
        if len(time)<3 : 
            continue

        year  = int(time[0])
        month = int(time[1])
        date  = int(time[2])


        picklong = round(float (words[6]),3)
        picklat  = round(float (words[7]),3)
        droplong = round(float (words[8]),3)
        droplat  = round(float (words[9]),3)
        passengers = int(words[10])

        if picklong == 0.0 or picklat == 0.0 or droplong == 0.0 or droplat == 0.0:
            continue
        # fare = float(words[2])

    
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print '%d,%.3f,%.3f,%.3f,%.3f:%d' % (month,picklong,picklat,droplong,droplat,passengers)
    except:
        continue

