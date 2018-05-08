
# coding: utf-8

# In[ ]:

#!/usr/bin/python

import sys
import string

currentkey = None
# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

	#Remove leading and trailing whitespace
	line = line.strip()
	#Get key/value 
	key, value = line.split('\t',1)

	#Parse key/value input
	value = value.split(',')
	if key == currentkey:
		for i in range(7):
			if value[i] != 'null':
				counts[i] += 1
				values[i] += float(value[i])
	else:
		if currentkey:
			output = ['null']*7
			for i in range(7):
				if counts[i] != 0:
					output[i] = '{0:.4f}'.format(values[i]/counts[i])
			output = ','.join(output)
			print('{0}\t{1}'.format(currentkey, output))
			#print('{0}\t{1}'.format(currentkey, ','.join(output)))

		currentkey = key
		values = [0,0,0,0,0,0,0]
		counts = [0,0,0,0,0,0,0]

output = ['null']*7
for i in range(7):
	if counts[i] != 0:
		output[i] = '{0:.4f}'.format(values[i]/counts[i])
output = ','.join(output)
print('{0}\t{1}'.format(currentkey, output))
#print('{0}\t{1}'.format(currentkey, ','.join(output)))


