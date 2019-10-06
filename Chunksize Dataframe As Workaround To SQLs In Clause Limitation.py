# -*- coding: utf-8 -*-
"""
Spyder Editor

This script shows how to chunksize when reading into pandas at 1000
in order to get around SQL's in clause limitation of 1000. The steps
to this are as follows:
    1. read data into the dataframe and chunksize it at 1000
    2. create an empty dataframe called pull that will be loaded
       with the data that will be pulled
    2. loop through the chunks and for each chunk create a list
        of what goes into the in clause
    3. Combine the list with a sql statement and append the data to
       the dataframe pull and load chunk to unchunked dataframe
    4. Merge data frames unchunked and pull. on the id field.

"""

import pandas as pd
import pyodbc

#Create SQL Server databaseconnection
con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=SQLEXPRESS;"
                      "Database=Sales;"
                      "Trusted_Connection=yes;MARS_Connection=Yes;")

#Read in database data an chunksize at 1000
df = pd.read_sql("SELECT id,year,issue_d,final_d from Datasets..loan_final", con, chunksize=1000)

#Create empty dataframes to be used in chunk processing
pull = pd.DataFrame()
unchunked = pd.DataFrame()

#Declare and set i as 0 to use to identify the chunk that is being processed to monitor progress
i = 0
for chunk in df:
    #increment i
    i += 1
    #print i to show which chunk is currently being processed
    print(i)
    #Create a list for the in clause
    mylist = ", ".join("'" + str(name) + "'" for name in chunk['id'])
    #Combine list with a sql staetment to append to the pull dataframe
    pull = pull.append(pd.read_sql("SELECT id, INSTALLMENT FROM Datasets..loan_final where id in(" + mylist + ")", con))
    #Load the chunk into an unchunked dataframe to merge with pull later
    unchunked = unchunked.append(chunk)

#Merge data frames unchunked and pull
merged = pd.merge(unchunked, pull, on='id', how='inner')

#Review the head of the merged dataframe
print(merged.head())

