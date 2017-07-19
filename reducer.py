#!/usr/bin/env python3
import sys,os #, re

#inverted index
index={}

#Read input
for line in sys.stdin:
    #get word and document 
    word, posting = line.split()
    
    #Add values to index
    if word in index:
    	index[word]=index[word]+ ", " + posting
    else:
        index[word]=posting

#Send output to stdout
for word in index:
    print('{}\t{}'.format(word, index[word]))
