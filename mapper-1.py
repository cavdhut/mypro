#!/usr/bin/python
#Mapper program for assignment:  Find the most popular tags for each month
#Student Name: Chandan Avdhut
# Note:   Input CSV file had tab separated entries instead of ','.  So I have implemented program accordingly
#
import sys

# Order of values in each line in the input file
# The first line of the input file is the header
# Values in each line are separated by '\t'
Id = 0
Tags = 2
CreationDate = 13

# Read the input from the STDIN one line at a time
for line in sys.stdin:

    vals = line.split('\t')

    # Skip the first line of the input file which is the header
    if vals[Id] == 'Id':
        continue

    # Skip entry if tags are not present
    if vals[Tags] == '-':
        continue
    else:
        #Print month and each tag to STDOUT
        for tag in vals[Tags][1:-1].split("><"):
            print '{}\t{}'.format(vals[CreationDate][:7], tag)
