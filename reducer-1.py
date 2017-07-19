#!/usr/bin/python
#Reducer program for assignment:  Find the most popular tags for each month
#Student Name: Chandan Avdhut
#
import sys
from collections import Counter

# key to keep track of months
last_key = None

#list to store tags for each month
tags=[]

# Read the the intermediate key-value records from the STDIN one record (line) at a time
for line in sys.stdin:

    # Split the line to extract the key and value parts of the record
    key, value = line.strip().split('\t', 1)

    # Compare the ket with the last key we have read so far to see if
    # all the records associated with th previous key are read and so a new key is being read
    if last_key != key:

        # If we are done reading the records associated with the previous key,
        # write month and most common tags (printing top 3 tags )
        if last_key:
            #find most common tags
            counter=Counter(tags)
            #print month and common tags to STDOUT
            print '{}\t{}'.format(last_key, " ".join(str(tag ) for tag in counter.most_common(3)))
            #Reset list
            tags=[]

        # Reset the auxiliary variables
        last_key = key
    #Add tag to List
    tags.append(value)

# Write last record
if last_key:
    print
    '{}\t{}'.format(last_key, " ".join(str(tag) for tag in counter.most_common(3)))