import numpy as np
import pandas as pd
import pyspark
import datetime
import time
import math as m
import matplotlib.pyplot as plt
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Big Data Analysis") \
                    .config("spark.some.config.option", "some-value") \
                    .getOrCreate()
                    

# Data Overview
def Data_Description (All_data1, All_data2): 
    
    time_start = datetime.datetime.now()
    print('The start time is: ', time_start, '\n')
   
    data1 = All_data1
    data2 = All_data2
   
    ## Count total row number in the two files
    TotalRow_Count_data1 = data1.count()
    TotalRow_Count_data2 = data2.count()
    print ('1. The total row number in the data1 file is:', TotalRow_Count_data1)
    print ('2. The total row number in the data2 file is:', TotalRow_Count_data2)
    
    
    ##### I. Check if the two data files match

    ## 1) Find the counts of unique observation ID in the data1 and data2
    data1_UniqID = data1.select('ID').distinct().count()
    data2_UniqID = data2.select('ID').distinct().count()
    
    ## 2) Check if data2 has duplicate IDs
    if data2_UniqID == TotalRow_Count_data2:
        print('3. The data2 file does not have duplicate IDs.')
    else:
        print('3. The data2 file has duplicate IDs. Further checks are needed.')

    ## 3) Check if the ID counts in the two files match
    if data1_UniqID == data2_UniqID:
        print('4. The ID counts in the two files are equale. The count is: ', data2_UniqID)
    else:
        print('4. The ID counts in the two files are not equale. Further checks are needed.')
    
    
    ##### II. Find the average/min/max number of waypoints per trip.
    
    ## Generate a frequency table from the data1 file with two columns: the 1st col is "ID", and the 2nd col is the count number
    ID_Freq = data1.groupby("ID").count() 

    ## Find the max,min,averge number of waypoints per trip based on the Waypoint file
    CountsSummary = ID_Freq.describe('count').toPandas()
    print('\n 5. The statistical summary of the data entry number per observation in the data1 file: \n', CountsSummary)
    
    
    ##### III. Add a column in data2 data frame to indicate the number of entries per observation.
   
    ## Merge the frequency table to the data1 file and drop the duplicate column   
    data2_Counts = data2.join(ID_Freq, data2.ID == ID_Freq.ID).drop(ID_Freq.ID)
    

    time_end = datetime.datetime.now()
    print('The ending time is: ', time_end, '\n')
    print('The running duration is: ', time_end - time_start)
    
    return data2_Counts


def Observation_Duration (data2_Counts)

    # Calculate the duration time for each observation. 
    # Add a column in the dataframe to indicate the duration. 
    # Find the average/min/max.
   
    timeFmt  = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    Duration = (F.unix_timestamp('EndDate', format = timeFmt) - F.unix_timestamp('StartDate', format = timeFmt))
   
    data2_Counts_Dur = data2_Counts.withColumn("Duration", Duration)
    
    DurationSummary = data2_Counts_Dur.describe("Duration").toPandas()
    print('\n 6. The statistical summary of the duration time (in seconds) per observation: \n', DurationSummary)



# ============================================================================
time_start = datetime.datetime.now()
print('The start time is: ', time_start, '\n')


## Read all the data1 files
Path_data1_1 = '../file1/data1.csv*'
Path_data1_2 = '../file2/data1.csv*'

DF_data1_1 = spark.read.csv(Path_data1_1)
DF_data1_2 = spark.read.csv(Path_data1_2)

## Read all the data2 files
Path_data2_1 = '../file1/data2_1.csv*'
Path_data2_2 = '../file2/data2_2.csv*'

DF_TripRcd1 = spark.read.csv(Path_data2_1)
DF_TripRcd2 = spark.read.csv(Path_data2_2)

## Merge all files into one file
All_data1 = DF_data1_1.union(data1_2)
All_data2 = DF_data2_1.union(data2_2)

time_end = datetime.datetime.now()
print('The ending time is: ', time_end, '\n')
print('The running duration is: ', time_end - time_start)
# ============================================================================


data2_Counts = Data_Description (All_data1, All_data2)    
Observation_Duration (data2_Counts)
