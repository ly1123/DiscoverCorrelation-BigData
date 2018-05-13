# DS 1004 Final Project DiscoverCorrelation-BigData

In this project, our goal is to discover potential correlations among datasets with different temporal and spatial resolutions. We first preprocess large quantity of data with spark and then we aggregate well-structured outputs based on diverse aggregation functions. For the final step, we compute pairwise mutulal information and pearson correlation to measure relationships among different attributes.  


# Dataset

The data that we used in this project are collected from NYC open data with total size of over 100 GB. There are eight data sets: 

- Yellow cab taxi data ranging from 2011 to 2017; 
- Vehicle collision data from 2011 to 2018; 
- 311 call service records from 2011 to 2017; 
- Citibike trip history data from 2010 to 2017; 
- Crime data from 2006 to 2016; 
- Weather data from 2011 to 2017; 
- Property data from 2012 to 2017; 
- Census data from 2013 to 2016. 

All datasets have spatial and temporal attributes respectively with different precisions. 

# Code
The code of this project are categorized into 4 parts, CodeSpark, MapReduce, SpatialResolution and Results.

## CodeSpark

Contains main python files that includes mappers.py, preprocess.py and correlation calculating files. 
- mapper.py contains all mapper functions are used to gather and clean information from 8 different ddata sets. 
- preprocess.py removes header of each dataset and run mapper functions from mapper.py to process and generate dataframe output with keys and values with the form of (year month day hour zip)
- corr_.py files takes two dataframes from different datasets, aggregate attributes with spark SQL, inner join two datasets with same keys and calculate correlations afterwards. 

## SpatialResolution

Contains 

## Results 

Contains the output form corr_.py files for different datasets. Both the join datasets and final result of mutual information and pearson correlations between two datasets. 
