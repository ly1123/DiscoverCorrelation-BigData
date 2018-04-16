#!/usr/bin/python

import sys
import string
import csv
import io 

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
	line = line.strip()
	Pre_string=io.StringIO(line)
	reader=csv.reader(Pre_string)
	for i in reader:
		entry=i
	if entry[0] == 'date':
		continue
	year = entry[0][:4]
	mon = entry[0][4:6]
	day = entry[0][6:8]
	h = entry[1][:2]
	spd = entry[2].strip()
	if spd == '999.9' or spd == '':
		spd = 'null'
	visb = entry[3].strip()
	if visb == '999999' or visb == '':
		visb = 'null'
	temp = entry[4].strip()
	if temp == '999.9' or temp == '':
		temp = 'null'
	prcp = entry[5].strip()
	if prcp == '999.9' or prcp == '':
		prcp = 'null'
	sd = entry[6].strip()
	if sd == '9999' or sd == '':
		sd = 'null'
	swd = entry[7].strip()
	if swd == '99999.9' or swd == '':
		swd = 'null'
	sa = entry[8].strip()
	if sa == '999' or sa == '':
		sa = 'null'
	print('{0},{1},{2},{3},{4},{5},{6},{7}\t{8},{9},{10},{11},{12},{13},{14}'.format(year, mon, day, h, 4, -1, -1, 1, spd, visb, temp, prcp, sd, swd, sa))

