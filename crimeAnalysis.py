# -*- coding: utf-8 -*-
"""
Created on Sat Apr 14 04:18:24 2018
@author: Shikha
1.	Keep data for NYC, Chicago, LA from 2010 to 2016
2.	Extract and create new cols namely address and zip code, using Lat,Lon values
3.	Classify /group similar crimes. Categorize time as- morning(4:00 AM to 11.59 AM), day(12:00 PM  to 5:00 PM),Evening (5:01 PM to 8 PM), night (8:01 PM to 12 AM), late night(12:01 AM - 3:59 AM).
4.	Time or year Seasons (Spring(March, April and May), summer(June, July, August), Fall(September, October, November), Winter(December, January, February)) 
5.	Make different pandas data frames for the specified cities
6.	Fields to include data frame- 
	Crime, Date Occurred, Time, Lat, Lon, Zip, Address, LocationType, Inside/Outside, VictimAge, VictimSex, Arrest, Seasons(Spring, summer, Fall), Violent(T,F)
"""
import json, csv
from geopy.geocoders import Nominatim
from pygeocoder import Geocoder
from geopy.exc import GeocoderTimedOut
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker
import plotly.plotly as py
import zipcode
from datetime import datetime as dt

#Coverting from date col to dateTime datatype
dateParser = lambda x,y: pd.datetime.strptime(str(x)+' '+str(y), '%m/%d/%Y %H:%M:%S')

ny_df = pd.read_csv("C:\\Users\\rohit\\Downloads\\nypdSample2010_15.csv","r", delimiter=',',encoding="utf-8-sig", parse_dates={'fromDate1': [1,2]}, date_parser=dateParser, error_bad_lines=False, warn_bad_lines=True)
ny_df['fromDate'] = ny_df.fromDate1.convert_objects(convert_dates='coerce')

ny_df = ny_df[['fromDate', 'RPT_DT', 'OFNS_DESC','CRM_ATPT_CPTD_CD', 'LAW_CAT_CD','BORO_NM', 'LOC_OF_OCCUR_DESC',
                 'PREM_TYP_DESC', 'Latitude','Longitude']].rename(columns={'RPT_DT': 'ReportDate', 'OFNS_DESC':'Crime',
                 'CRM_ATPT_CPTD_CD':'CrimeStatus', 'LAW_CAT_CD':'LawCategory','BORO_NM':'Area', 'LOC_OF_OCCUR_DESC':'Loc/In/Out',
                 'PREM_TYP_DESC':'PremiseDesc'})
ny_df.head()
len(ny_df)
ny_df.dtypes
list(ny_df.columns.values)

#Group similar crimes
crimeCnt = ny_df.groupby('Crime').agg({'Crime':['count']})
len(crimeCnt)
ny_df['CrimeGrpd'] = ny_df['Crime']
ny_df[(ny_df.Crime == 'NA')]['Crime'] = 'Other'

ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['SEX CRIMES'],'Sex Crimes')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['BURGLARY', "BURGLAR'S TOOLS"],'Burglary')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['THEFT-FRAUD', 'THEFT OF SERVICES', 'OTHER OFFENSES RELATED TO THEF'],'Theft')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['PETIT LARCENY', 'PETIT LARCENY OF MOTOR VEHICLE','GRAND LARCENY', 'GRAND LARCENY OF MOTOR VEHICLE','UNAUTHORIZED USE OF A VEHICLE'],'Larceny')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['NYS LAWS-UNCLASSIFIED FELONY', 'NYS LAWS-UNCLASSIFIED VIOLATION', 'AGRICULTURE & MRKTS LAW-UNCLASSIFIED'],'Unclassified')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['ALCOHOLIC BEVERAGE CONTROL LAW', 'INTOXICATED & IMPAIRED DRIVING','VEHICLE AND TRAFFIC LAWS'],'DUI/Traffic laws')

ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['DISORDERLY CONDUCT','OFFENSES AGAINST PUBLIC ADMINI','ENDAN WELFARE INCOMP','ANTICIPATORY OFFENSES' ,'OFFENSES AGAINST PUBLIC SAFETY','ANTICIPATORY OFFENSES','OFFENSES AGAINST THE PERSON','OFFENSES INVOLVING FRAUD','JOSTLING','OFF. AGNST PUB ORD SENSBLTY &','MISCELLANEOUS PENAL LAW'],'Penal law/Fraud/Safety')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['FORGERY','FRAUDS','FRAUDULENT ACCOSTING','ARSON','POSSESSION OF STOLEN PROPERTY'],'Asset Related')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['MURDER & NON-NEGL. MANSLAUGHTER','HOMICIDE','RAPE','HOMICIDE-NEGLIGENT-VEHICLE','ASSAULT 3 & RELATED OFFENSES'],'Violent')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['DANGEROUS WEAPONS'],'Weapon Related')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['HARRASSMENT 2'],'Harrassment')

ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['CHILD ABANDONMENT/NON SUPPORT','KIDNAPPING & RELATED OFFENSES','OFFENSES RELATED TO CHILDREN'],'Crime against children')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['CRIMINAL MISCHIEF & RELATED OF','CRIMINAL TRESPASS'],'Criminal Mischeif')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['PROSTITUTION & RELATED OFFENSES','GAMBLING'],'Victimless Crimes')

ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['ADMINISTRATIVE CODE','ADMINISTRATIVE CODES'],'Administrative')
ny_df['CrimeGrpd'] = ny_df['CrimeGrpd'].replace(['ADMINISTRATIVE CODE','ADMINISTRATIVE CODES'],'Administrative')

crimeCt = ny_df.groupby('CrimeGrpd').agg({'CrimeGrpd':['count']})
len(crimeCt)

#Add new columns for Month and Year in data frame
ny_df['Month'] = np.asarray(ny_df['fromDate'].dt.month)
ny_df['Year'] = np.asarray(ny_df['fromDate'].dt.year)

#Keep data subset from Year 2010 to 2015
ny_df = ny_df.loc[ny_df['Year'].between(2010,2015)]

#Add col Seasons, Spring(March, April and May), summer(June, July, August), 
#Fall(September, October, November), Winter(December, January, February)  
ny_df['Seasons'] = 'winter'
ny_df.loc[ny_df['Month'] > 2, 'Seasons'] = 'spring'
ny_df.loc[ny_df['Month'] > 5, 'Seasons'] = 'summmer'
ny_df.loc[ny_df['Month'] > 8, 'Seasons'] = 'fall'
ny_df.loc[ny_df['Month'] > 11, 'Seasons'] = 'winter'

#Add col TimeOfDay as- morning(4:00 AM to 11 AM), day(12:00 PM  to 17:00 PM),
#Evening (18:00 PM to 20 PM), night (21 PM to 23 AM), late night(00 AM - 3 AM)
ny_df['TimeHour'] = ny_df.fromDate.apply(lambda x: x.hour)
#ny_df.ix[:15, ['fromDate','TimeHour','TimeOfDay']]

ny_df.loc[ny_df['TimeHour'].between(12,17), 'TimeOfDay'] = 'day'
ny_df.loc[ny_df['TimeHour'].between(18,20), 'TimeOfDay'] = 'evening'
ny_df.loc[ny_df['TimeHour'].between(21,23), 'TimeOfDay'] = 'night'
ny_df.loc[ny_df['TimeHour'].between(0,3), 'TimeOfDay'] = 'late night'
ny_df.loc[ny_df['TimeHour'].between(4,11), 'TimeOfDay'] = 'morning'

ny_df.to_csv("NewYorkCrimeData.csv", sep='\t', encoding='utf-8')

#Plots##################################################################
#done
#Year wise trend of crimes in each area of NYC
yearVScrime = ny_df.groupby(['Year','Area'], as_index=False).agg({'CrimeGrpd':['count']})
yearVScrime.columns = ['Year','Area','Count']

BronxCrimes = yearVScrime[yearVScrime['Area'] =="BRONX"]
BrookCrimes = yearVScrime[yearVScrime['Area'] == "BROOKLYN"]
manhatCrimes = yearVScrime[yearVScrime['Area'] =="MANHATTAN"]
queensCrimes = yearVScrime[yearVScrime['Area'] == "QUEENS"]
staIslndCrimes = yearVScrime[yearVScrime['Area'] == "STATEN ISLAND"]

BronxCrimes.columns = ['Year', 'Area' , 'Count']
BrookCrimes.columns = ['Year', 'Area' , 'Count']
manhatCrimes.columns = ['Year', 'Area' , 'Count']
queensCrimes.columns = ['Year', 'Area' , 'Count']
staIslndCrimes.columns = ['Year', 'Area' , 'Count']

fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(nrows=2, ncols=2)
ax1.set_title('Bronx crime trend')
ax1.plot(BronxCrimes['Year'], BronxCrimes['Count'])
#ax1.set_xlabel('Year')
ax1.set_ylabel('Frequency')

ax2.plot(BrookCrimes['Year'], BrookCrimes['Count'])
ax2.set_title('Brook crime trend')

ax3.plot(manhatCrimes['Year'], manhatCrimes['Count'])
ax3.set_title('Manhattan crime trend')
ax3.set_xlabel('Year')
ax3.set_ylabel('Frequency')

ax4.plot(queensCrimes['Year'], queensCrimes['Count'])
ax4.set_title('Queens crime trend')
ax4.set_xlabel('Year')

plt.suptitle('Year vs crimes in each area of NYC',fontsize= 15)
plt.subplots_adjust(hspace = 0.5,top = 0.8 )
plt.style.use('ggplot')
plt.tight_layout()
plt.savefig("YearwiseCrimeTrendPerArea.png")

#ax5.plot(staIslndCrimes['Year'], staIslndCrimes['Count'])
#ax5.set_title('Yearly crime trend in  STATEN ISLAND')

#done
#Time of day when most crimes happen in each area
crimeVSTime = ny_df.groupby(['Area','TimeOfDay'], as_index=False).agg({'CrimeGrpd':['count']})
crimeVSTime.columns = [ 'Area', 'TimeOfDay', 'TotalCrimes']
crimeVSTime = crimeVSTime.pivot(index='Area', columns='TimeOfDay', values='TotalCrimes')
crimeVSTime.plot(kind='bar',figsize=(10,4))
plt.title(f"Area at each time of day vs. Total Crimes")
plt.xlabel("Area with each time of day")
plt.ylabel("Total number of Crimes")
plt.xticks(rotation='vertical')
plt.style.use('ggplot')
plt.savefig("crimeVSTimeOfDay.png")
plt.show()

#done
#Num of crimes in each area  
crimeStatVSarea = ny_df.groupby(['Area','CrimeStatus'], as_index=False).agg({'CrimeGrpd':['count'] })
crimeStatVSarea.columns = ['Area','CrimesStatus','CrimeCount']
crimeStatVSarea = crimeStatVSarea.sort_values(by = 'CrimesStatus', ascending=False)

plt.bar(crimeStatVSarea['Area'], crimeStatVSarea['CrimeCount'])
plt.title(f"Area vs. Total Crimes")
plt.xlabel("Area")
plt.ylabel("Total number of Crimes")
plt.xticks(rotation='vertical')
plt.style.use('ggplot')
plt.savefig("areaVScrimeCount.png")
plt.show()

#done
#Num of complete or incomplete crimes in each area 
#reshaping the data
crimeStatVSarea = crimeStatVSarea.pivot(values='CrimeCount', index='Area', columns='CrimesStatus')
crimeStatVSarea.plot(kind='bar',figsize=(10,4))

ax = plt.gca()
pos = []
for bar in ax.patches:
    pos.append(bar.get_x()+bar.get_width()/2.)
ax.set_xticks(pos,minor=True)
lab = []
for i in range(len(pos)):
    l = crimeStatVSarea.columns.values[i//len(crimeStatVSarea.CrimesStatus.values)]
    lab.append(l)

ax.set_xticklabels(lab,minor=True)
ax.tick_params(axis='x', which='major', pad=15, size=0)
plt.setp(ax.get_xticklabels(), rotation=0)
plt.show()
plt.savefig("crimeStatusVSarea.png")
plt.show()

#Done
#Crime percentage w.r.t seasons in NYC
SeasonsVSCrime = ny_df.groupby('Seasons', as_index=False).agg({'CrimeGrpd':['count']})
SeasonsVSCrime.columns = [ 'Seasons', 'TotalCrimes']
# Pie chart, where the slices will be ordered and plotted counter-clockwise:
labels = SeasonsVSCrime['Seasons']
sizes = SeasonsVSCrime['TotalCrimes']
explode = (0, 0.1, 0, 0)  # only "explode" the 2nd slice 

fig1, ax1 = plt.subplots()
ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',shadow=True, startangle=90)
ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.tight_layout()
ax1.set_title('Crime percentage w.r.t seasons in NYC')
#ax1.ylabel('Frequency of Crimes')
plt.savefig("SeasonsVSCrime.png")
plt.show()

#Done
#Premise Descriptions vs crimes 
PremiseVSCrimes = ny_df.groupby('PremiseDesc')['PremiseDesc'].agg({'Crime':['count']}).reset_index()
PremiseVSCrimes.columns = ['PremiseDesc','TotalCrimes']
PremiseVSCrimes = PremiseVSCrimes.sort_values(by = 'TotalCrimes', ascending=False).head(15)

plt.bar(PremiseVSCrimes['PremiseDesc'],PremiseVSCrimes['TotalCrimes'])
plt.title(f"Premise Description vs. Total Crimes")
plt.xlabel("Premise Description")
plt.xticks(rotation='vertical')
plt.ylabel("Total number of Crimes")
plt.style.use('ggplot')
plt.savefig("PremiseVSCrimes.png")
plt.show()

#done
#Top three crimes in each Area
TimeOfDayVSCrime = ny_df.groupby(['Area','CrimeGrpd'], as_index=False).agg({'Crime':['count']})
TimeOfDayVSCrime.columns = ['Area','Crimes','CrimeCount']
TimeOfDayVSCrime1 = pd.DataFrame(TimeOfDayVSCrime.groupby(['Area'])['CrimeCount'].nlargest(3).reset_index(level=1,drop=True)).reset_index() 
TimeOfDayVSCrime2 = pd.merge(TimeOfDayVSCrime1, TimeOfDayVSCrime, how='left', on=['Area','CrimeCount'])

TimeOfDayVSCrime2 = TimeOfDayVSCrime2.pivot(values='CrimeCount', index='Area', columns='Crimes')
TimeOfDayVSCrime2.plot(kind='bar',figsize=(10,4))

plt.title(f"Top three type of crimes in each Area ")
plt.xlabel("Area with top 3 type of crimes")
plt.xticks(rotation='vertical')
plt.ylabel("Total number of Crimes")
plt.style.use('ggplot')
plt.savefig("prevalantCrimesPerArea.png")
plt.show()

#done
#Top Law Categories for reported crimes in each area of NYC
LawCategoryVSCrime = ny_df.groupby(['Area','LawCategory'], as_index=False).agg({'CrimeGrpd':['count']})
LawCategoryVSCrime.columns = ['Area', 'LawCategory', 'CrimeCount']

LawCategoryVSCrime = LawCategoryVSCrime.pivot(values='CrimeCount', index='Area', columns='LawCategory')
LawCategoryVSCrime.plot(kind='bar',figsize=(10,4))

plt.title(f"Law Category vs. Total Crimes per Area  of New York")
plt.xlabel("Area with Law Categories")
plt.xticks(rotation='vertical')
plt.ylabel("Total number of Crimes from Yr 2010 - 2015")
#plt.style.use('ggplot')
plt.savefig("LawCategoryVSCrime.png")
plt.show()

#do
#crimeVSTime = ny_df.groupby(['Area','TimeOfDay'], as_index=False).agg({'CrimeGrpd':['count']})
#crimeVSTime.columns = [ 'Area', 'TimeOfDay', 'TotalCrimes']
#crimeVSTime = crimeVSTime.pivot(index='Area', columns='TimeOfDay', values='TotalCrimes')
#crimeVSTime.plot(kind='bar',figsize=(10,4))
###########################################################################################

#done
#Time of day vs number of crimes
TimeOfDayVSCrime = ny_df.groupby('TimeOfDay').agg({'Crime':['count']}).reset_index()
TimeOfDayVSCrime.columns = [ 'TimeOfDay', 'Frequency']
TimeOfDayVSCrime = TimeOfDayVSCrime.sort_values(by = 'Frequency', ascending=False).head(10)

plt.bar(TimeOfDayVSCrime['TimeOfDay'],TimeOfDayVSCrime['Frequency'])
plt.title(f"Time Of Day vs. Total Crimes")
plt.xlabel("Time Of Day")
plt.xticks(rotation='vertical')
plt.ylabel("Total Crimes from Yr (2010 - 2015)")
plt.style.use('ggplot')
plt.savefig("TimeOfDayVSCrime.png")
plt.show()

#Done
#Top 3 crimes w.r.t time of day in each area

