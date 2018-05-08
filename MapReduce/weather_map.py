#!/usr/bin/python

# this is the map function for weather data
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
    # skip header
	if entry[0] == 'date':
		continue
    #date and time
	year = entry[0][:4]
	mon = entry[0][4:6]
	day = entry[0][6:8]
	h = entry[1][:2]
    spd = entry[2].strip() #The wind speed rate
	if spd == '999.9' or spd == '':
		spd = 'null'
    visb = entry[3].strip() #The horizontal distance
	if visb == '999999' or visb == '':
		visb = 'null'
    temp = entry[4].strip() #The temperature of the air
	if temp == '999.9' or temp == '':
		temp = 'null'
    prcp = entry[5].strip() #The depth of liquid precipitation
	if prcp == '999.9' or prcp == '':
		prcp = 'null'
    sd = entry[6].strip() #The depth of snow and ice
	if sd == '9999' or sd == '':
		sd = 'null'
    swd = entry[7].strip() #The depth of the liquid content of snow precipitation
	if swd == '99999.9' or swd == '':
		swd = 'null'
    sa = entry[8].strip() #The depth of a snow accumulation
	if sa == '999' or sa == '':
		sa = 'null'
	print('{0},{1},{2},{3},{4},{5},{6},{7}\t{8},{9},{10},{11},{12},{13},{14}'.format(year, mon, day, h, 4, -1, -1, 1, spd, visb, temp, prcp, sd, swd, sa))

