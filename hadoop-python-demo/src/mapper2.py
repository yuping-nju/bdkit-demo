#!/usr/bin/env python
 
import sys
import string

# delete punctuations
def clean(s):
    return ''.join(c for c in s if not c in string.punctuation)

 
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    # split the line into words
    line = str(clean(line.lower()))
    words = line.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print '%s\t%s' % (word, 1)