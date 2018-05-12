#!/usr/bin/python

import string
import numpy as np
from shapely.geometry import Polygon
from shapely.geometry import Point
import pickle

def build_grid(polys, xs, ys):
'''
Build grid search according to polygons of corresponding districts
'''
	grid_dict = {}
	polys_bounds = []
	xmin = 10000
	xmax = -10000
	ymin = 10000
	ymax = -10000
	for poly in polys:
		bound = poly.bounds
		polys_bounds.append(bound)
		xmin = min(bound[0], xmin)
		ymin = min(bound[1], ymin)
		xmax = max(bound[2], xmax)	
		ymax = max(bound[3], ymax)
	width = (xmax-xmin)/xs
	height = (ymax-ymin)/ys
	low_bound = (xmin, ymin, width, height)
	for k, bound in enumerate(polys_bounds):	
		stx = getIndex(bound[0], xmin, width)
		enx = getIndex(bound[2], xmin, width) + 1
		sty = getIndex(bound[1], ymin, height)
		eny = getIndex(bound[3], ymin, height) + 1
		if enx > xs:	
			enx = xs
		if eny > ys:
			eny = ys
		for i in range(stx, enx):
			for j in range(sty, eny):
				if grid_dict.get((i,j)):
					grid_dict[(i,j)].append(k)
				else:	
					grid_dict[(i,j)] = [k, ]
	return grid_dict, low_bound

def getIndex(x, xmin, length):
	index = (x-xmin)/length
	return int(index)

def find_dist(point, grid_dict, polys, polys_dict, low_bound):
'''
return corresponding district given the point
grid_dict: dict with key=grid and value=index of polygons
polys: list of polygons
polys_dict: dict with key=index of polygons and value=index of district
low_bound: the bound of New York city
'''
	xmin = low_bound[0]
	ymin = low_bound[1]
	width = low_bound[2]
	height = low_bound[3]
	index_x = getIndex(point[0], xmin, width)
	index_y = getIndex(point[1], ymin, height)
	point = Point(point)
	if grid_dict.get((index_x, index_y)):
		grid_polys = grid_dict[(index_x, index_y)]
		if len(grid_polys) == 1:
			return polys_dict[grid_polys[0]]
		else: 
			for i in grid_polys:
				if polys[i].contains(point):
					return polys_dict[i]
	return None 
	
if __name__ == "__main__":
	# get polygons_to_zipcode dict and list of polygons 
	'''
	with open('zip.pickle', 'rb') as f:
		zipcode = pickle.load(f)
	with open('nei.pickle', 'rb') as f:
		neibor = pickle.load(f)	
	polys_zipcode = []
	polys_zipcode_dict = {}
	i = 0
	for key, values in zipcode.items():
		for poly in values:
			polys_zipcode.append(poly)
			polys_zipcode_dict[i]= key
			i += 1
	with open('polys_zipcode.pickle', 'wb') as f:
		pickle.dump(polys_zipcode, f)	
	with open('polys_zipcode_dict.pickle', 'wb') as f:
		pickle.dump(polys_zipcode_dict, f)

	point = (-74.002950346, 40.749717753)
	polys_neibor = []
	polys_neibor_dict = {}
	i = 0
	for key, values in neibor.items():
		for poly in values:
			polys_neibor.append(poly)
			polys_neibor_dict[i]= key
			i += 1
	with open('polys_neibor.pickle', 'wb') as f:
		pickle.dump(polys_neibor, f)	
	with open('polys_neibor_dict.pickle', 'wb') as f:
		pickle.dump(polys_neibor_dict, f)
	'''
	
	# build grid search and test this method
	point = (-74.002950346, 40.749717753)
	xs = 10000
	ys = 10000
	with open('polys_zip.pickle', 'rb') as f:
		polys_zipcode = pickle.load(f)	
	with open('polys_zip_dict.pickle', 'rb') as f:
		polys_zipcode_dict = pickle.load(f)
	grid_zipcode_dict, bound_zipcode= build_grid(polys_zipcode, xs, ys) #build grid search for zipcode
	with open('grid_zip_dict.pickle', 'wb') as f:
		pickle.dump(grid_zipcode_dict, f)
	with open('grid_zip_dict.pickle', 'rb') as f:
		grid_zipcode_dict = pickle.load(f)
	with open('bound_zip.pickle', 'wb') as f:
		pickle.dump(bound_zipcode, f)
	with open('bound_zip.pickle', 'rb') as f:
		bound_zipcode = pickle.load(f)
	print(find_dist(point, grid_zipcode_dict, polys_zipcode, polys_zipcode_dict, bound_zipcode))
	
	with open('polys_neibor.pickle', 'rb') as f:
		polys_neibor = pickle.load(f)	
	with open('polys_neibor_dict.pickle', 'rb') as f:
		polys_neibor_dict = pickle.load(f)
	grid_neibor_dict, bound_neibor = build_grid(polys_neibor, xs, ys) #build grid search for neighborhood
	with open('grid_neibor_dict.pickle', 'wb') as f:
		pickle.dump(grid_neibor_dict, f)
	with open('grid_neibor_dict.pickle', 'rb') as f:
		grid_neibor_dict = pickle.load(f)
	with open('bound_neibor.pickle', 'wb') as f:
		pickle.dump(bound_neibor, f)
	with open('bound_neibor.pickle', 'rb') as f:
		bound_neibor = pickle.load(f)
	print(find_dist(point, grid_neibor_dict, polys_neibor, polys_neibor_dict, bound_neibor))

