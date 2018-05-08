#!/usr/bin/python 

import numpy as np
from csv import reader
import io
from gridSearch import find_dist 
import time
from pyspark.sql import Row

def mapper_citibike(line, zipcode=None, neibor=None):
	line = line.strip()
	Pre_string=io.StringIO(line)
	reads=reader(Pre_string)
	for i in reads:
		entry=i
	dt = entry[0].strip() #trip duration
	try: # deal with missing values
		dt = float(dt)
	except:
		dt = None
	try:
		stime = time.strptime(entry[1].strip(), '%Y-%m-%d %H:%M:%S') #start time for a trip
	except:
		try:
			stime = time.strptime(entry[1].strip(), '%m/%d/%Y %H:%M:%S')
		except:
			stime = (0, 0, 0, 0)
	syear = int(stime[0]) #year
	smon = int(stime[1]) #month
	sday = int(stime[2]) #day
	sh = int(stime[3]) #hour
	sst = entry[3].strip() #start station id
	if sst == '':
		sst = None
	sp = (float(entry[6].strip()), float(entry[5].strip())) # the coordinate for the start station
	zipcode = find_dist(sp, zipcode)
	est = entry[7].strip() #end station id 
	if est == '':
		est = None
	bid = entry[11].strip() #bike id
	ut = entry[-3].strip() #user type
	biry = entry[-2].strip() #birth year
	try:
		biry = int(biry)
	except:
		biry = None
	gd = int(entry[-1].strip()) #gender
	key = '{0:d} {1:d} {2:d} {3:d} {4:d} {5:d}'.format(syear, smon, sday, sh, zipcode, -1)
	return Row(key=key, start_sta_id=sst, end_sta_id=est, bike_id=bid, user_type=ut, gender=gd, birth_year=biry, duration_time=dt)

def mapper_weather(line, zipcode=None, neibor=None):
	line = line.strip()	
	Pre_string=io.StringIO(line)
	reads=reader(Pre_string)
	for i in reads:
		entry=i
	stime = time.strptime(entry[0].strip(), '%Y%m%d') #start time for a trip
	syear = stime[0]
	smon = stime[1]
	sday = stime[2]
	h = entry[1][:2]
	h = int(h)
	spd = entry[2].strip() #The wind speed rate
	if spd == '999.9' or spd == '':
                spd = None
	spd = float(spd) 
	visb = entry[3].strip() #The horizontal distance at which an object can be seen and identified
	if visb == '999999' or visb == '':
		visb = None
	visb = float(visb)
	temp = entry[4].strip() #The temperature of the air
	if temp == '999.9' or temp == '':
		temp = None 
	temp = float(temp)	
	prcp = entry[5].strip() #The depth of liquid precipitation 
	if prcp == '999.9' or prcp == '':
		prcp = None
	prcp = float(prcp)
	'''
	sd = entry[6].strip() #The depth of snow and ice on the ground
	if sd == '9999' or sd == '':
		sd = None
	swd = entry[7].strip() #The depth of the liquid content of snow precipitation that has accumulated on the ground
	if swd == '99999.9' or swd == '':
		swd = None
	sa = entry[8].strip() #The depth of a snow accumulation
	if sa == '999' or sa == '':	
		sa = None
	'''
	key = '{0:d} {1:d} {2:d} {3:d}'.format(syear, smon, sday, h)
	return Row(key=key, spd=spd, visb=visb, temp=temp, prcp=prcp)


def mapper_property(line, zipcode=None, neibor=None):
	line = line.strip()
	Pre_string=io.StringIO(line)
	reads=reader(Pre_string) 
	for i in reads:
		entry=i    
	BBLE = entry[0].strip()
	if BBLE == '':
		BBLE = None
    #"B","Byte","borough valid values 1-5"
	B = entry[1].strip()
	if B == '':
		B = None
    #"BLOCK","Long Integer",
	BLOCK  = entry[2].strip()
	if BLOCK == '':
		BLOCK = None
    #"LOT","Long Integer",
	LOT = entry[3].strip()
	if LOT == '':
		LOT = None
    #"OWNER","Text","owner name"
	OWNER = entry[5].strip()
	if OWNER == '':
		OWNER = None   
    #"BLDGCL","Text","building class"
	BLDGCL = entry[6].strip()
	if BLDGCL == '':
		BLDGCL = None
    #"TAXCLASS","Text","tax class"
	TAXCLASS = entry[7].strip()
	if TAXCLASS == '':
		TAXCLASS = None
    #"LTFRONT","Text","lot width"
	LTFRONT = entry [8].strip()
	if LTFRONT == '0':
		LTFRONT = None  
    #LTDEP","Text","lot depth"
	LTDEPTH = entry [9].strip()
	if int(LTDEPTH) == 0:
		LTDEPTH = None  
    #"STORIES","Text","number of stories in building"
	STORIES = entry[11].strip()	
	if STORIES =='':
		STORIES = None      
    #"FULLVAL","Double","market value"
	FULLVAL = entry[12].strip()
	if FULLVAL == '':
		FULLVAL = None
    #"AVLAND","Double","actual land value"
	AVLAND = entry[13].strip()
	if AVLAND == '':
		AVLAND = None
    #"AVTOT","Double","actual total value"
	AVTOT = entry[14].strip()
	if AVTOT == '':
		AVTOT = None
    #"EXLAND","Double","actual exempt land value"
	EXLAND = entry[15].strip()
	if EXLAND == '':
		EXLAND = None
    #"EXTOT","Double","actual exemtp land total"
	EXTOT = entry[16].strip()
	if EXTOT == '':
		EXTOT = None
    #"EXCD1","Text","exemption code 1"
	EXCD1 = entry[17].strip()
	if EXCD1 =='':
		EXCD1 = None  
    #"STADDR","Text","street address"
	STADDR = entry[18].strip()
	if STADDR == '':
		STADDR = None
    #"ZIP","Text","zip code"
	ZIP = entry[19].strip()
	if ZIP == '':
		ZIP = -1
	ZIP = int(ZIP)    
#"EXMPTCL","Text","exempt class"
	EXMPTCL = entry[20].strip()
	if EXMPTCL == '':
		EXMPTCL = None  
    #"BLDFRONT","Text","building width"
	BLDFRONT = entry[21].strip()
	if BLDFRONT =='0':
		BLDFRONT = None    
    #"BLDDEPTH","Text","building depth"
	BLDDEPTH = entry[22].strip()
	if BLDDEPTH == '0':
		BLDDEPTH = None  
    #"AVLAND2","Double","transitional land value"
	AVLAND2 = entry[23].strip()
	if AVLAND2 == '':
		AVLAND2 = None     
    #"AVTOT2","Double","transitional total value"
	AVTOT2 = entry[24].strip()
	if AVTOT2 == '':
		AVTOT2 = None  
    #"EXLAND2","Double","transitional exempt land value"
	EXLAND2 =entry[25].strip()
	if EXLAND2 == '':
		EXLAND2 = None  
    #"EXTOT2","Double","transitional exempt land total"
	EXTOT2 =entry[26].strip()
	if EXTOT2 == '':
		EXTOT2 = None  
    #"EXCD2","Text","exemption code 2"
	EXCD2 =entry[27].strip()
	if EXCD2 == '':
		EXCD2 = None    
    #"PERIOD","Text","assessment period when file was created"
	PERIOD =entry[28].strip()
	if PERIOD == '':
		PERIOD = None
	syear = int(entry[29].strip()[:4])+1
    #"YEAR","Text","assessment year"
	'''   
	try:
		stime = time.strptime(entry[29].strip(), '%Y/%m') 
		syear = stime[0]
	except:
		syear = 0
	'''
    #syear = int(entry[29].strip()[:4])+1
	VALTYPE =entry[30].strip()
	if VALTYPE =='':
		VALTYPE = None  

	key = '{0:d} {1:d}'.format(syear, ZIP)
	return Row(key = key , bble=BBLE, Byte=B, Block=BLOCK, Lot=LOT, Owner=OWNER, Building_number=BLDGCL, Tax_Class=TAXCLASS, Street_address=STADDR, Exempt_Class=EXMPTCL, Periodd=PERIOD, Val_Type=VALTYPE, Lot_Width=LTFRONT, Lot_Depth=LTDEPTH, Stories=STORIES, Full_Value=FULLVAL, Land_Value=AVLAND, Total_value=AVTOT, Exempt_Land_Value=EXLAND, Exempt_Total_Value=EXTOT, Exempt_Code=EXCD1, Building_Width=BLDFRONT, Building_Depth=BLDDEPTH,Trans_Land_Value=AVLAND2, Trans_Total_Value=AVTOT2, Trans_exempt_landvalue=EXLAND2, Trans_totalvalue=EXTOT2, Exempt_Code2=EXCD2) 


def mapper_collision(line, zipcode=None, neibor=None):
	line = line.strip()
	Pre_string=io.StringIO(line)
	reads=reader(Pre_string)
	for i in reads:
		entry=i
	#DATE
	if len(entry)<29: #deal with abnormal records
		key = '{0:d} {1:d} {2:d} {3:d} {4:d} {5:d}'.format(0, 0, 0, 0, -1, -1)
		return Row(key=key, BOROUGH=None, ON_STREET_NAME=None, CROSS_STREET_NAME=None, OFF_STREET_NAME=None, NUMBER_OF_PERSONS_INJURED=None, NUMBER_OF_PERSONS_KILLED=None, NUMBER_OF_PEDESTRIANS_INJURED=None, NUMBER_OF_PEDESTRIANS_KILLED=None, NUMBER_OF_CYCLIST_INJURED=None, NUMBER_OF_CYCLIST_KILLED=None, NUMBER_OF_MOTORIST_INJURED=None, NUMBER_OF_MOTORIST_KILLED=None, UNIQUE_KEY=None) 
	try:
		stime = time.strptime(entry[0].strip(), '%m/%d/%Y') 
	except:
		stime = (0, 0, 0)
	syear= int(stime[0])
	smon = int(stime[1])
	sday = int(stime[2]) 
	#TIME
	try:
		sstime = time.strptime(entry[1].strip(), '%H:%M')
	except:
		sstime = (0,0)
	sh = int(sstime[0]/100)

	#BOROUGH  
	BOROUGH  = entry[2].strip()
	if BOROUGH =='':
		BOROUGH = None
	#ZIP CODE 
	ZIP  = entry[3].strip()
	if ZIP == '':
		ZIP = -1
	ZIP = int(ZIP)       
    
 	#  ON STREET NAME   
	onstreet  = entry[7].strip()
	if onstreet  =='':
		onstreet  = None
        #CROSS STREET NAME  
	crossstreet = entry[8].strip()
	if crossstreet  =='':
		crossstreet  = None
	#OFF STREET NAME  
	offstreet = entry[9].strip()
	if offstreet  =='':
		offstreet  = None
        
	#NUMBER OF PERSONS INJURED  
	perinjure = entry[10].strip()
	if perinjure  =='':
		perinjure = None


	#NUMBER OF PERSONS KILLED   
	perkill = entry[11].strip()
	if perkill  =='':
		perkill = None

	#NUMBER OF PEDESTRIANS INJURED   
	pedinjure = entry[12].strip()
	if pedinjure  =='':
		pedinjure = None

	#NUMBER OF PEDESTRIANS KILLED  
	pedkill = entry[13].strip()
	if pedkill  =='':
		pedkill = None

	#NUMBER OF CYCLIST INJURED   
	cycinjure = entry[14].strip()
	if cycinjure   =='':
		cycinjure = None
    
	#NUMBER OF CYCLIST KILLED  
	cyckill = entry[15].strip()
	if cyckill  =='':
		cyckill = None

	#NUMBER OF MOTORIST INJURED  
	motoinjure = entry[16].strip()
	if motoinjure =='':
		motoinjure = None
	#NUMBER OF MOTORIST KILLED 
	motokill = entry[17].strip()
	if motokill  =='':
		motokill = None
	'''    
	CONTRIBUTING FACTOR VEHICLE18 1   
	CONTRIBUTING FACTOR VEHICLE19 2   
	CONTRIBUTING FACTOR VEHICLE20 3   
	CONTRIBUTING FACTOR VEHICLE21 4   
	CONTRIBUTING FACTOR VEHICLE22 5 
	'''
	#UNIQUE KEY   
	unikey = entry[23].strip()
	if unikey  =='':
		unikey = None  
	'''        
	VEHICLE TYPE CODE 1   
	VEHICLE TYPE CODE 2   
	VEHICLE TYPE CODE 3   
	VEHICLE TYPE CODE 4   
	VEHICLE TYPE CODE 5
	'''
 
	key = '{0:d} {1:d} {2:d} {3:d} {4:d} {5:d}'.format(syear, smon, sday, sh, ZIP, -1)
	return Row(key=key, BOROUGH=BOROUGH, ON_STREET_NAME=onstreet, CROSS_STREET_NAME=crossstreet, OFF_STREET_NAME=offstreet, NUMBER_OF_PERSONS_INJURED=perinjure, NUMBER_OF_PERSONS_KILLED=perkill, NUMBER_OF_PEDESTRIANS_INJURED=pedinjure, NUMBER_OF_PEDESTRIANS_KILLED=pedkill, NUMBER_OF_CYCLIST_INJURED=cycinjure, NUMBER_OF_CYCLIST_KILLED=cyckill, NUMBER_OF_MOTORIST_INJURED=motoinjure, NUMBER_OF_MOTORIST_KILLED=motokill, UNIQUE_KEY=unikey) 


def mapper_311(line, zipcode=None, neibor=None):
	line = line.strip()
	Pre_string=io.StringIO(line)
	reads=reader(Pre_string)
	for i in reads:
		entry=i
	stime = time.strptime(entry[1].strip(), '%m/%d/%Y %H:%M:%S %p') #start time
	syear = stime[0]
	smon = stime[1]
	sday = stime[2]
	sh = stime[3]
	agency = entry[3].strip() #agency
	if agency =='':
		agency = None
	complaint_type = entry[5].strip()
	if complaint_type =='':
		complaint_type = None
	location_type = entry[7].strip()
	if location_type =='N/A':
		location_type = None
	_zip = entry[8].strip()
	try:
		_zip = int(_zip)
	except:
		_zip = -1
	incident_add = entry[9].strip()
	if incident_add =='':
		incident_add = None
	cross_st_1 = entry[11].strip()
	if cross_st_1 == '':
		cross_st_1 = None
	cross_st_2 = entry[12].strip()
	if cross_st_2 == '':
		cross_st_2 = None
	city = entry[16].strip()
	try:
		city = str(city)
	except:
		city = None
	status = entry[19].strip() #status
	sp = entry[52].strip() #coordinate
	if sp =='':
		sp = None
	key = '{0:d} {1:d} {2:d} {3:d} {4:d}'.format(syear, smon, sday, sh, _zip)
	return Row(key=key, agency=agency, complaint_type=complaint_type, location_type=location_type, incident_add=incident_add, cross_st_1 = cross_st_1, cross_st_2 = cross_st_2, city=city, status=status, sp=sp)


def mapper_crime(line, zipcode=None, neibor=None):		
	line = line.strip()
	Pre_string=io.StringIO(line)
	reads=reader(Pre_string)
	for i in reads:
		entry=i
	CMPLNT_NUM=entry[0]
	CMPLNT_FR_DT=entry[1]
	CMPLNT_FR_TM=entry[2]
	CMPLNT_TO_DT=entry[3]
	CMPLNT_TO_TM=entry[4]
	RPT_DT=entry[5]
	KY_CD=entry[6] #Three digit offense classification code
	OFNS_DESC=entry[7]
	PD_CD=entry[8] # Three digit internal classification code (more granular than Key Code)
	PD_DESC=entry[9]
	CRM_ATPT_CPTD_CD=entry[10] # Indicator of whether crime was successfully completed or attempted, but failed or was interrupted prematurely
	LAW_CAT_CD=entry[11] # Level of offense: felony, misdemeanor, violation
	JURIS_DESC=entry[12] 
	BORO_NM=entry[13]
	ADDR_PCT_CD=entry[14] #The precinct in which the incident occurred 
	LOC_OF_OCCUR_DESC=entry[15] 
	PREM_TYP_DESC=entry[16]
	PARKS_NM=entry[17]
	HADEVELOPT=entry[18]
	X_COORD_CD=entry[19]
	Y_COORD_CD=entry[20]
	Latitude=entry[21]
	Longitude=entry[22]
	Lat_Lon=entry[23]
	Created_Year=CMPLNT_FR_DT[6:10]
	Created_Day=CMPLNT_FR_DT[3:5]
	Created_Month=CMPLNT_FR_DT[0:2]
	Created_Hour=CMPLNT_FR_TM[0:2]
	key = '{0:d} {1:d} {2:d} {3:d} {4:d}'.format(Created_Year, Created_Month, Created_Day, Created_Hour, -1)
	return Row(key=key, KY_CD=KY_CD, PD_CD=PD_CD, LAW_CAT_CD=LAW_CAT_CD, ADDR_PCT_CD=ADDR_PCT_CD, CRM_ATPT_CPTD_CD=CRM_ATPT_CPTD_CD)
