#!/usr/bin/env python3
import sys,os#, re

#Read the input
for line in sys.stdin:
    #get input file name	
    doc_path = os.environ["map_input_file"]
    doc_id = doc_path.split("/")[-1]
    
    #create map of  word and document 
    for word in line.split():
        print ('{}\t{}'.format(word, doc_id))
