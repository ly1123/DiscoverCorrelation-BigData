
# coding: utf-8

# In[ ]:

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
    #avoid the title line 
	for i in reader:
		entry=i
	if entry[0] == 'BBLE':
		continue
    #"BBLE","Text","one field with bble in text format"   
    BBLE = entry[0].strip()
    #"B","Byte","borough valid values 1-5"
    B = entry[1].strip()
    #"BLOCK","Long Integer",
    Blk = entry[2].strip()
    #"LOT","Long Integer",
    Lot = entry[3].strip()
    #"OWNER","Text","owner name"
    Owner = entry[5].strip()
    if Owner == '':
        Owner = 'Null'
    #"BLDGCL","Text","building class"
    BLDGCL = entry[6].strip()
    #"TAXCLASS","Text","tax class"
    TAXCLASS = entry[7].strip()
    #"LTFRONT","Text","lot width"
    LTFRONT = entry [8].strip()
    if LTFRONT == 0:
        LTFRONT = 'NULL'
    #LTDEP","Text","lot depth"
    LTDEP = entry [9].strip()
    if LTDEP == 0:
        LTDEP = 'NULL'
    #"STORIES","Text","number of stories in building"
    STORIES = entry[11].strip()
    if STORIES =='':
        STORIES = 'NULL'    
    #"FULLVAL","Double","market value"
    FULLVAL = entry[12].strip()
    #"AVLAND","Double","actual land value"
    AVLAND = entry[13].strip()
    #"AVTOT","Double","actual total value"
    AVTOT entry[14].strip()
    #"EXLAND","Double","actual exempt land value"
    EXLAND = entry[15].strip()
    #"EXTOT","Double","actual exemtp land total"
    EXTOT = entry[16].strip()
    #"EXCD1","Text","exemption code 1"
    EXCD1 = entry[17].strip()
    if EXCD1 =='':
        EXCD1 = 'NULL'
    #"STADDR","Text","street address"
    STADDR = entry[18].strip()
    #"ZIP","Text","zip code"
    ZIP = entry[19].strip()
    if ZIP == '':
        ZIP = 'NULL'
    #"EXMPTCL","Text","exempt class"
    EXMPTCL = entry[20].strip()
    if EXMPTCL == '':
        EXMPTCL = 'NULL'
    #"BLDFRONT","Text","building width"
    BLDFRONT = entry[21].strip()
    if BLDFRONT == 0:
        BLDFRONT = 'NULL'   
    #"BLDDEPTH","Text","building depth"
    BLDDEPTH = entry[22].strip()
    if BLDDEPTH == 0:
        BLDDEPTH = 'NULL' 
    #"AVLAND2","Double","transitional land value"
    AVLAND2 = entry[23].strip()
    #"AVTOT2","Double","transitional total value"
    AVTOT2 = entry[24].strip()
    #"EXLAND2","Double","transitional exempt land value"
    EXLAND2 =entry[25].strip()
    if EXLAND2 == '':
        EXLAND2 = 'NULL'
    #"EXTOT2","Double","transitional exempt land total"
    EXTOT2 =entry[26].strip()
    if EXTOT2 == '':
        EXTOT2 = 'NULL'
    #"EXCD2","Text","exemption code 2"
    EXCD2 =entry[27].strip()
    if EXCD2 == '':
        EXCD2 = 'NULL'    
    #"PERIOD","Text","assessment period when file was created"
    PERIOD =entry[28].strip()
    #"YEAR","Text","assessment year"   
	year = entry[29][:4]
	mon = entry[29][4:6]
	day = entry[29][6:8]
    h= 0

	print('{0},{1},{2},{3},{4},{5},{6},{7}\t{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24},{25},{26},{27},{28},{29},{30},{31},{32},{33}'.format(year, mon, day, h, 3, -1, -1, 1, BBLE, B, BLOCK, LOT, OWNER, BLDGCL, TAXCLASS, LTFRONT, LTDEPTH, STORIES, FULLVAL, AVLAND, AVTOT, EXLAND, EXTOT, EXCD1, STADDR, ZIP, EXMPTCL, BLDFRONT, BLDDEPTH, AVLAND2, AVTOT2, EXLAND2, EXTOT2, EXCD2, PERIOD, YEAR, ALTYPE))


