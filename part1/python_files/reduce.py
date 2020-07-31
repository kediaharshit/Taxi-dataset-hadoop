#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None
lst = [[(0,0)]*6]*13 

def func (a,b) :
    words = a.split(',')
    index =int(words[0])
    global lst
    lst[index][5] = (a,b)
    lst[index] = sorted(lst[index],key= lambda c : c[1],reverse=True)

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split(':')

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            # print '%s\t%s' % (current_word, current_count)
            func(current_word, current_count)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    func(current_word, current_count)
for j in range(1,13):
    for i in range(0,5):
        print lst[j][i]