from pyspark.sql.types import *
from __future__ import division
maxTempArray = []
minTempArray = []
maximumTempArray = []
minimumTempArray = []
file_path1 = "/data/weather/2010"
file_path2 = "/data/weather/2011"
file_path3 = "/data/weather/2012"
file_path4 = "/data/weather/2013"
file_path5 = "/data/weather/2014"
file_path6 = "/data/weather/2015"
file_path7 = "/data/weather/2016"
file_path8 = "/data/weather/2017"
file_path9 = "/data/weather/2018"
file_path10 = "/data/weather/2019"
inTextData1 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path1)
inTextData2 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path2)
inTextData3 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path3)
inTextData4 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path4)
inTextData5 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path5)
inTextData6 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path6)
inTextData7 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path7)
inTextData8 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path8)
inTextData9 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path9)
inTextData10 = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path10)

//DROPING COLUMNS
def dropColumns(x):
  columns = x.split(" ")
  output = columns[0] + " " + columns[2] + " " + columns[3] + " " + columns[5] + " " + columns[7] + " " + columns[9] + " " + columns[11] + " " + columns[13] + " " + columns[15] + " " + columns[16] + " " + columns[17] + " " + columns[18] + " " + columns[19] + " " + columns[20] + " " + columns[21]
  return output

//FILTERING TEMP
def filterTemp(x):
   columns = x.split(" ")
   if(columns[2] == "9999.9"):
     return False
   else:
     return True

//FILTERING MAX
def filterMax(x):
   columns = x.split(" ")
   if(columns[10] == "9999.9"):
     return False
   else:
     return True
	 
//FILTERING MIN
def filterMin(x):
   columns = x.split(" ")
   if(columns[11] == "9999.9"):
     return False
   else:
     return True

//FILTERING GUST
def filterGust(x):
   columns = x.split(" ")
   if(columns[9] == "999.9"):
     return False
   else:
     return True
	 
//FILTERING PRECIPITATION
def filterPrep(x):
   columns = x.split(" ")
   if(columns[12] == "99.99"):
     return False
   else:
     return True
	 
//TASK1 MAPPING
def task1Map(x):
  columns = x.split(" ")
  return (columns[2], columns[0]+" "+columns[1])
  
//TASK1 MAX MAPPING
def task1MaxMap(x):
  columns = x.split(" ")
  return (float(columns[10][:-1]), columns[0]+" "+columns[1])
  
//TASK1 MIN MAPPING
def task1MinMap(x):
  columns = x.split(" ")
  return (float(columns[11][:-1]), columns[0]+" "+columns[1])

//TASK3 MAPPING
def task3Map(x):
  columns = x.split(" ")
  return (float(columns[12][:-1]), columns[0]+" "+columns[1])
  
//TASK4 MAPPING
def task4Filter(x):
  columns = x.split(" ")
  if(columns[5] == "9999.9"):
    return True
  else:
	return False
  
//TASK5 MAPPING
def task5Map(x):
  columns = x.split(" ")
  return (columns[9], columns[0]+" "+columns[1])

rdd1 = inTextData1.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)
rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_1 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_1)
task1Min_1 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_1)

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_1 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_1)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_1 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_1)

rdd1 = inTextData2.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)
rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_2 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_2)
task1Min_2 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_2)

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_2 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_2)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_2 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_2)

rdd1 = inTextData3.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)
rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_3 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_3)
task1Min_3 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_3)

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_3 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_3)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_3 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_3)

rdd1 = inTextData4.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)
rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_4 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_4)
task1Min_4 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_4)

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_4 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_4)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_4 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_4)

rdd1 = inTextData5.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)
rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_5 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_5)
task1Min_5 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_5)

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_5 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_5)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_5 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_5)

rdd1 = inTextData6.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)
rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_6 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_6)
task1Min_6 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_6)

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_6 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_6)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_6 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_6)

rdd7 = rdd6.filter(filterPrep)
task3M = rdd7.map(task3Map)
task3Max = task3M.max(key = lambda x: x[0])
task3Min = task3M.min(key = lambda x: x[0])

rdd1 = inTextData7.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)
rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_7 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_7)
task1Min_7 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_7)

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_7 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_7)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_7 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_7)

rdd1 = inTextData8.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)
rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_8 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_8)
task1Min_8 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_8)

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_8 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_8)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_8 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_8)

rdd1 = inTextData9.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)
rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_9 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_9)
task1Min_9 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_9)

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_9 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_9)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_9 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_9)

rdd1 = inTextData10.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])

rdd5 = rdd4.map(lambda x: x.replace("'",""))
rdd6 = rdd5.map(dropColumns)

total_count = rdd6.count()
task4M = rdd6.filter(task4Filter)
missing_count = task4M.count()
percentage = (missing_count/total_count)*100

rdd7 = rdd6.filter(filterTemp)

task1M = rdd7.map(task1Map)
task1Max_10 = task1M.max(key = lambda x: float(x[0]))
maxTempArray.append(task1Max_10)
task1Min_10 = task1M.min(key = lambda x: float(x[0]))
minTempArray.append(task1Min_10)

rdd7 = rdd6.filter(filterGust)
task5M = rdd7.map(task5Map)
task5Max_10 = task5M.max(key = lambda x: float(x[0]))

rdd7 = rdd6.filter(filterMax)

task1M = rdd7.map(task1MaxMap)
task1Maximum_10 = task1M.max(key = lambda x: float(x[0]))
maximumTempArray.append(task1Maximum_10)

rdd7 = rdd6.filter(filterMin)

task1M = rdd7.map(task1MinMap)
task1Minimum_10 = task1M.min(key = lambda x: float(x[0]))
minimumTempArray.append(task1Minimum_10)

//TASK2.1
min = minTempArray[0]
max = maxTempArray[0]
for x in range(1, 10):
  if(float(maxTempArray[x][0]) > float(max[0])):
    max = maxTempArray[x]
  if(float(minTempArray[x][0]) < float(min[0])):
    min = minTempArray[x]
	
//TASK2.2
minimum = minimumTempArray[0]
maximum = maximumTempArray[0]
for x in range(1, 10):
  if(float(maximumTempArray[x][0]) > float(maximum[0])):
    maximum = maximumTempArray[x]
  if(float(minimumTempArray[x][0]) < float(minimum[0])):
    minimum = minimumTempArray[x]
  
s='2010 Hottest(max,stn date): ' + str(task1Max_1), '2010 Coldest(min,stn date): ' +str(task1Min_1),'2010 Maximum(max,stn date): ' + str(task1Maximum_1),'2010 Minimum(min, stn date): ' + str(task1Minimum_1),'2011 Hottest(max,stn date): ' + str(task1Max_2),'2011 Coldest(min,stn date): ' + str(task1Min_2),'2011 Maximum(max,stn date): ' + str(task1Maximum_2),'2011 Minimum(min, stn date): ' + str(task1Minimum_2),'2012 Hottest(max,stn date): ' + str(task1Max_3),'2012 Coldest(min,stn date): ' + str(task1Min_3),'2012 Maximum(max,stn date): ' + str(task1Maximum_3),'2012 Minimum(min, stn date): ' + str(task1Minimum_3),'2013 Hottest(max,stn date): ' + str(task1Max_4),'2013 Coldest(min,stn date): ' + str(task1Min_4),'2013 Maximum(max,stn date): ' + str(task1Maximum_4),'2013 Minimum(min, stn date): ' + str(task1Minimum_4), '2014 Hottest(max,stn date): ' + str(task1Max_5), '2014 Coldest(min,stn date): ' + str(task1Min_5), '2014 Maximum(max,stn date): ' + str(task1Maximum_5),'2014 Minimum(min, stn date): ' + str(task1Minimum_5),'2015 Hottest(max,stn date): ' + str(task1Max_6), '2015 Coldest(min,stn date): ' + str(task1Min_6), '2015 Maximum(max,stn date): ' + str(task1Maximum_6),'2015 Minimum(min, stn date): ' + str(task1Minimum_6),'2016 Hottest(max,stn date): ' + str(task1Max_7), '2016 Coldest(min,stn date): ' + str(task1Min_7), '2016 Maximum(max,stn date): ' + str(task1Maximum_7),'2016 Minimum(min, stn date): ' + str(task1Minimum_7),'2017 Hottest(max,stn date): ' + str(task1Max_8), '2017 Coldest(min,stn date): ' + str(task1Min_8), '2017 Maximum(max,stn date): ' + str(task1Maximum_8),'2017 Minimum(min, stn date): ' + str(task1Minimum_8),'2018 Hottest(max,stn date): ' + str(task1Max_9), '2018 Coldest(min,stn date): ' + str(task1Min_9), '2018 Maximum(max,stn date): ' + str(task1Maximum_9),'2018 Minimum(min, stn date): ' + str(task1Minimum_9),'2019 Hottest(max,stn date): ' + str(task1Max_10), '2019 Coldest(min,stn date): ' + str(task1Min_10), '2019 Maximum(max,stn date): ' + str(task1Maximum_10),'2019 Minimum(min, stn date): ' + str(task1Minimum_10),'Overall Hottest:' + str(max), 'Overall Coldest' + str(min) , 'Overall Maximum' + str(maximum),'Overall Minimum' + str(minimum),'2015 Maximum Precipitation: ' + str(task3Max), '2015 Mimimum Precipitation: ' + str(task3Min),'2019 Missing STP Percentage: ' + str(percentage),'2019 Maximum Wind Gust: ' + str(task5Max_10)

rdd=sc.parallelize(s)
rdd=rdd.coalesce(1)
//saving to csv
rdd.saveAsTextFile('Result')