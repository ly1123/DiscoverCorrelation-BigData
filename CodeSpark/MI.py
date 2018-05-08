#!/usr/bin/python 

from math import log
log2= lambda x:log(x,2)
from collections import defaultdict
import pandas as pd

def mutual_information(x, y, x_d ,y_d, bins=10):
    '''mutual information for variable x and y
    input: x and y are vectors with the same length; x_d and y_d are indicators whether x and y need binning; bins is the number of bins
    output: the mutual information value
    '''
    if x_d ==1:
        x = pd.cut(x, bins=bins, labels=False)
    if y_d ==1:
        y = pd.cut(y, bins=bins,  labels=False)
    return entropy(y) - conditional_entropy(x,y)

def conditional_entropy(x, y):
    '''
    compute H(Y|X)
    '''
    Py= compute_distribution(y)
    Px= compute_distribution(x)
    res= 0
    for ey in set(y):
        # P(X | Y)
        x1= x[y==ey]
        condPxy= compute_distribution(x1)

        for k, v in condPxy.items():
            res+= (v*Py[ey]*(log2(Px[k]) - log2(v*Py[ey])))
    return res
        
def entropy(y):
    '''
    Compute entropy of y
    '''
    Py= compute_distribution(y)
    res=0.0
    for k, v in Py.items():
        res+=v*log2(v)
    return -res

def compute_distribution(v):
    '''
    compute distribution of v
    input: a vector
    output: a dict
    '''
    d= defaultdict(int)
    for e in v: d[e]+=1
    s= float(sum(d.values()))
    return dict((k, v/s) for k, v in d.items())

