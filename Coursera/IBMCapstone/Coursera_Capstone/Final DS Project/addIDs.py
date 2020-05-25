#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 10:00:11 2017

@author: stjohn
"""

#Open all the files:
inZone = open('zoning.json', 'r')
outZone = open('zoningIDS.json', 'w')
outCSV = open('zoneDist.csv', 'w')

#Counter for creating ID numbers:
countID = 1

#Create columns for CSV file:
outCSV.write("arbID,Zoning District\n")

#For each line in the original file:
for line in inZone:
    #find where the zone district is:
    z = line.find(' "ZONE')
    if z > -1:
        #If the line contains zone district, add in an arbitrary ID just before it:
        newLine = line[:z] + '"arbID": ' + str(countID) + ", " + line[z:]
        #Write to the new json outfile:
        outZone.write(newLine)
        
        #Find the zoning district
        zz = line[z+14:].find('"')
        #Write the arbitrary ID and it to the new CSV file:
        outCSV.write(str(countID)+", " + line[z+14:z+14+zz]+"\n")
        
        #Increment the counter:
        countID += 1
    else: #ZONEDIST isn't in the line, so, we should copy it over unchanged:
        outZone.write(line)

#Close the files when finished:
inZone.close()
outZone.close()
outCSV.close()