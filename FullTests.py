
# coding: utf-8

# In[370]:

from decimal import Decimal
import pandas as pd
import pymysql
import logger
import math
import sys
import os
import calendar
from datetime import date, datetime, timedelta
import time
import json
from interana_client import Client, Query, InteranaError, SINGLE_MEASURE
import warnings
from random import randint
warnings.filterwarnings('ignore')

# In[371]:

def print_full(x):
    pd.set_option('display.max_rows', len(x))
    print(x)
    pd.reset_option('display.max_rows')


HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query1():
    start = datetime.utcnow() - timedelta(14)
    end = datetime.utcnow()
    query = Query('sonos_usage', start, end)

    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='unique_count',
        column='SonosID',
        filter=' `Usage.MusicServiceID` not in ("2048") and `Usage.PlayHours` not in ("0")'
    
    )
    
    query.add_params(
        max_groups=10000,
        group_by=["SonosID"],
        sampled=True
    )    
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query1())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def returnRandom(results): # dict results
    records = []
    random = randint(0, 1000)
    row = results.get("rows")[random]
    return (row.get("values")[0])

randomID = str(returnRandom(result._response))
randomID = randomID[1:-1]


print("-------------- Sonos ID: " + randomID + " --------------")
print

# In[372]:

def connect2():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetFlatPlayHoursByCustomerByDay',(randomID,))
    result = cur.fetchall()
    return result

result = connect2()

def format_pandas2(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["Day"] = str(row[2])
        ph = float(row[1])
        playHours = round(ph, 4)
        formatted["PlayHours"] = str(playHours)
        formatted["Day of Week"] = str(row[3])        
        if (playHours != 0.0000):
            records.append(formatted)  
        
    return json.dumps(records)
    pandas_json = format_pandas2(result._response)
    
pandas_json = format_pandas2(result)
deltron = pd.read_json(pandas_json, orient = "records")

# In[373]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query2():
    start = datetime.utcnow() - timedelta(14)
    end = datetime.utcnow()
    query = Query('sonos_usage', start, end)

    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)

    )
    
    query.add_params(
        max_groups=10000,
        group_by=["SonosID", "__minute__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    #make call to RandomFiveSonosID function
    result = api_client.query(create_query2())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    

def formatPandas2(results): # dict results
    records = []
    bucket = {}
    flatHours = {}
    for row in results.get("rows"):
        try:
            time = row.get("values")[0][1]
            time = round(math.floor((time/60/30/1000)))*60*30*1000
            rounded = datetime(1970, 1, 1) + timedelta(seconds=(time/1000.))
            dateTimeFmt = "%Y-%m-%d %H:%M"
            dateTime = rounded.strftime(dateTimeFmt)
            playHours = float(row.get("values")[1])
            playHours = round(playHours, 4)
            if dateTime in bucket:
                bucket[dateTime] = max(bucket[dateTime], playHours)
            else:
                bucket[dateTime] = playHours
        except IndexError:
            pass
        
    for key in bucket:
        date = key.split()[0]
        if date in flatHours:
            flatHours[date] = flatHours[date] + bucket[key]
        else:
            flatHours[date] = bucket[key]
            
    for key in flatHours:
        formatted = {}
        formatted["Day"] = str(key)
        formatted["PlayHours"] = str(flatHours[key])
        formatted["Day of Week"] = datetime.strptime(key, '%Y-%m-%d').strftime('%A')
        if (flatHours[key] != 0.0000):
            records.append(formatted)
    return json.dumps(records)

pandas_json = formatPandas2(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for FlatPlayHoursByCustomerByDay

# In[374]:
print("\n")
print("----FlatPlayHoursByCustomerByDay----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for FlatPlayHoursByCustomerByDay

print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[278]:

def connect3():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetFlatPlayHoursByCustomerByDayExcludeTV',(randomID,))
    result = cur.fetchall()
    return result

result = connect3()

def format_pandas3(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["Day"] = str(row[2])
        ph = float(row[1])
        playHours = round(ph, 4)
        formatted["PlayHours"] = str(playHours)
        formatted["Day of Week"] = str(row[3])        
        if (playHours != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas3(result._response)
    
#format_pandas(result)
pandas_json = format_pandas3(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[279]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query3():
    start = datetime.utcnow() - timedelta(14)
    end = datetime.utcnow()
    query = Query('sonos_usage', start, end)

    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    
    query.add_params(
        max_groups=10000,
        group_by=["SonosID", "__minute__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query3())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas3(results): # dict results
    records = []
    bucket = {}
    flatHours = {}
    for row in results.get("rows"):
        try:
            time = row.get("values")[0][1]
            original = datetime(1970, 1, 1) + timedelta(seconds=(time/1000.))
            time = round(math.floor((time/60/30/1000)))*60*30*1000
            rounded = datetime(1970, 1, 1) + timedelta(seconds=(time/1000.))
            dateTimeFmt = "%Y-%m-%d %H:%M"
            dateTime = rounded.strftime(dateTimeFmt)
            originalDateTime = original.strftime(dateTimeFmt)
            playHours = float(row.get("values")[1])
            playHours = round(playHours, 4)
            if dateTime in bucket:
                bucket[dateTime] = max(bucket[dateTime], playHours)
            else:
                bucket[dateTime] = playHours
        except IndexError:
            pass
        
    for key in bucket:
        date = key.split()[0]
        if date in flatHours:
            flatHours[date] = flatHours[date] + bucket[key]
        else:
            flatHours[date] = bucket[key]
            
    for key in flatHours:
        formatted = {}
        formatted["Date"] = str(key)
        formatted["PlayHours"] = str(flatHours[key])
        formatted["Day of Week"] = datetime.strptime(key, '%Y-%m-%d').strftime('%A')
        if (flatHours[key] != 0.0000):
            records.append(formatted)
    return json.dumps(records)

pandas_json = formatPandas3(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for FlatPlayHoursByCustomerByDayExcludeTV

print("\n")
print("----FlatPlayHoursByCustomerByDayExcludeTV----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for FlatPlayHoursByCustomerByDayExcludeTV

print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[282]:

def connect4():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetLifetimePercentageHoursByCustomerByRoom',(randomID,))
    result = cur.fetchall()
    return result

result = connect4()

def format_pandas4(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["Room"] = row[1]
        formatted["Percentage"] = str(row[3])
        ph = float(row[2])
        playHours = round(ph, 4)
        formatted["PlayHours"] = str(playHours)
        if (playHours != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas4(result._response)
    
#format_pandas(result)
pandas_json = format_pandas4(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[283]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query4():
    start = datetime(2016, 8, 1)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","SerialNumber.Config.RoomName"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query4())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas4(results): # dict results
    records = []
    totalHours = 0.0;
    for row in results.get("rows"):
    	totalHours = totalHours + (float)(row.get("values")[1])
    totalHours = round(totalHours,4)
    for row in results.get("rows"):
        formatted = {}
        if int(row.get("values")[1]) != 0:
            playHours = float(row.get("values")[1])
            playHours = round(playHours, 4)
            formatted["PlayHours"] = str(playHours)
            formatted["Percentage"] = str((((int)(row.get("values")[1]))/totalHours)*100)
            formatted["Room"] = row.get("values")[0][1]
            if (playHours != 0.0000):
                records.append(formatted)            
    return json.dumps(records)
    
pandas_json = formatPandas4(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for LifetimePercentageHoursByCustomerByRoom
print("\n")
print("----LifetimePercentageHoursByCustomerByRoom----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for LifetimePercentageHoursByCustomerByRoom

print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[286]:

def connect5():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetLifetimePlayHoursByCustomer',(randomID,))
    result = cur.fetchall()
    return result

result = connect5()

def format_pandas5(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        playHours = float(row[1])
        playHours = round(playHours, 4)
        formatted["PlayHours"] = str(row[1])
        if (playHours != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas5(result._response)
    
#format_pandas(result)
pandas_json = format_pandas5(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[287]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query5():
    start = datetime(2016, 8, 1)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query5())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas5(results): # dict results
    records = []
    totalHours = 0.0;
    for row in results.get("rows"):
        totalHours = totalHours + (float)(row.get("values")[1])
    formatted = {}
    totalHours = round(totalHours, 4)
    formatted["PlayHours"] = totalHours
    if (totalHours != 0.0000):
        records.append(formatted)            
    return json.dumps(records)    

pandas_json = formatPandas5(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for LifetimePlayHoursByCustomer
print("\n")
print("----LifetimePlayHoursByCustomer----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for LifetimePlayHoursByCustomer

print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[316]:

def connect6():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetLifetimePlayHoursByCustomerByRoom',(randomID,))
    result = cur.fetchall()
    return result

result = connect6()

def format_pandas6(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["RoomName"] = row[2]
        playHours = float(row[1])
        playHours = round(playHours, 4)
        formatted["PlayHours"] = playHours
        if (playHours != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas6(result._response)
    
#format_pandas(result)
pandas_json = format_pandas6(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[317]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query6():
    start = datetime(2016, 8, 1)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)

    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","SerialNumber.Config.RoomName"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query6())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    

def formatPandas6(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {}
        playHours = float(row.get("values")[1])
        playHours = round(playHours, 4)
        formatted["PlayHours"] = playHours
        formatted["Room"] = row.get("values")[0][1]
        if (playHours != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    
pandas_json = formatPandas6(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for LifetimePlayHoursByCustomerByRoom
print("\n")
print("----LifetimePlayHoursByCustomerByRoom----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for LifetimePlayHoursByCustomerByRoom

print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)



# In[321]:

def connect7():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetLifetimePlayHoursByCustomerByService',(randomID,))
    result = cur.fetchall()
    return result

result = connect7()

def format_pandas7(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["RoomName"] = row[2]
        playHours = float(row[1])
        playHours = round(playHours, 4)
        formatted["PlayHours"] = playHours
        if (playHours != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas7(result._response)
    
#format_pandas(result)
pandas_json = format_pandas7(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[322]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query7():
    start = datetime(2016, 8, 1)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)

    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","Usage.MusicServiceID.serviceName"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query7())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas7(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {} 
        try:
            if row.get("values")[0][1] != "*null*":
                formatted["Service"] = row.get("values")[0][1]
                playHours = float(row.get("values")[1])
                playHours = round(playHours, 4)
                formatted["PlayHours"] = playHours
                if (playHours != 0.0000):
                    records.append(formatted)       
        except IndexError:
            print("null")    
    return json.dumps(records)
    
pandas_json = formatPandas7(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for LifetimePlayHoursByCustomerByService
print("\n")
print("----LifetimePlayHoursByCustomerByService----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for LifetimePlayHoursByCustomerByService

print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[342]:

def connect8():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayHoursByCustomerByDay',(randomID,))
    result = cur.fetchall()
    return result

result = connect8()

def format_pandas8(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["Day"] = str(row[2])
        ph = float(row[1])
        playHours = round(ph, 4)
        formatted["PlayHours"] = str(playHours)
        if (playHours != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas8(result._response)
    
#format_pandas(result)
pandas_json = format_pandas8(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[343]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query8():
    start = datetime.now() - timedelta(60)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)

    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID", "__day__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query8())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas8(results): # dict results
    records = []
    dictionary = {}
    for row in results.get("rows"):
        try:
            formatted = {}
            s = row.get("values")[0][1]
            t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
            fmt = "%Y-%m-%d"
            day = t.strftime(fmt)
            playHours = float(row.get("values")[1])
            playHours = round(playHours, 4)
            formatted["Playhours"] = playHours
            formatted["Day"] = day
            if (playHours != 0.0000):
                records.append(formatted)
        except:
            pass
    return json.dumps(records)
    
pandas_json = formatPandas8(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayHoursByCustomerByDay
print("\n")
print("----PlayHoursByCustomerByDay----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayHoursByCustomerByDay

print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[346]:

def connect9():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayHoursByCustomerByDayOfWeek',(randomID,))
    result = cur.fetchall()
    return result

result = connect9()

def format_pandas9(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["Day of Week"] = str(row[2])
        ph = float(row[1])
        playHours = round(ph, 4)
        formatted["PlayHours"] = str(playHours)
        if (playHours != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas9(result._response)
    
#format_pandas(result)
pandas_json = format_pandas9(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[347]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query9():
    start = datetime.now() - timedelta(60)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID", "__day__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query9())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas9(results): # dict results
    records = []
    dictionary = {}
    for row in results.get("rows"):
        playHours = float(row.get("values")[1])
        playHours = round(playHours, 4)
        s = row.get("values")[0][1]
        t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
        fmt = "%Y-%m-%d"
        date = t.strftime(fmt)
        dayOfWeek = datetime.strptime(date, '%Y-%m-%d').strftime('%A')       
        if (dayOfWeek in dictionary):
            dictionary[dayOfWeek] = dictionary[dayOfWeek] + playHours
        else:
            dictionary[dayOfWeek] = playHours
    for key in dictionary:
        formatted = {}
        formatted["Playhours"] = dictionary[key]
        formatted["Day"] = key
        if (dictionary[key] != 0.0000):
            records.append(formatted) 

    return json.dumps(records)
    
pandas_json = formatPandas9(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayHoursByCustomerByDayOfWeek
print("\n")
print("----PlayHoursByCustomerByDayOfWeek----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayHoursByCustomerByDayOfWeek
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[355]:

def connect10():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayHoursByCustomerByRoomByDay',(randomID,))
    result = cur.fetchall()
    return result

result = connect10()

def format_pandas10(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["RoomName"] = row[2]
        formatted["Day"] = str(row[3])
        playHours = float(row[1])
        playHours = round(playHours, 4)
        formatted["PlayHours"] = str(playHours)
        records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas10(result._response)
    
#format_pandas(result)
pandas_json = format_pandas10(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[ ]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query10():
    start = datetime.now() - timedelta(60)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","SerialNumber.Config.RoomName", "__day__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query10())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas10(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {}
        formatted["RoomName"] = row.get("values")[0][1]
        s = row.get("values")[0][2]
        t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
        fmt = "%Y-%m-%d"
        formatted["Day"] = t.strftime(fmt)
        playHours = float(row.get("values")[1])
        playHours = round(playHours, 4)
        formatted["Playhours"] = playHours
        if (row.get("values")[1] != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    
pandas_json = formatPandas10(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayHoursByCustomerByRoomByDay
print("\n")
print("----PlayHoursByCustomerByRoomByDay----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayHoursByCustomerByRoomByDay
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[ ]:

def connect11():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayHoursByCustomerByRoomByDayOfWeek',(randomID,))
    result = cur.fetchall()
    return result

result = connect11()

def format_pandas11(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["RoomName"] = row[2]
        formatted["Day of Week"] = str(row[3])
        ph = float(row[1])
        playHours = round(ph, 4)
        formatted["PlayHours"] = str(playHours)
        records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas11(result._response)
    
#format_pandas(result)
pandas_json = format_pandas11(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[358]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query11():
    start = datetime(2016, 8, 1)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)

    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","SerialNumber.Config.RoomName", "day_of_week"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query11())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas11(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {}
        formatted["RoomName"] = row.get("values")[0][1]
        dayOfWeek = ""
        if int(row.get("values")[0][2]) == 1:
            dayOfWeek = "Monday"
        elif int(row.get("values")[0][2]) == 2:
            dayOfWeek = "Tuesday"
        elif int(row.get("values")[0][2]) == 3:
            dayOfWeek = "Wednesday"
        elif int(row.get("values")[0][2]) == 4:
            dayOfWeek = "Thursday"
        elif int(row.get("values")[0][2]) == 5:
            dayOfWeek = "Friday"
        elif int(row.get("values")[0][2]) == 6:
            dayOfWeek = "Saturday"
        elif int(row.get("values")[0][2]) == 7:
            dayOfWeek = "Sunday"
        formatted["Day of Week"] = dayOfWeek
        playHours = float(row.get("values")[1])
        playHours = round(playHours, 4)
        formatted["Playhours"] = playHours        
        if (row.get("values")[1] != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    
pandas_json = formatPandas11(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayHoursByCustomerByRoomByDayOfWeek
print("\n")
print("----PlayHoursByCustomerByRoomByDayOfWeek----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayHoursByCustomerByRoomByDayOfWeek
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[365]:

def connect12():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayHoursByCustomerByRoomByMonth',(randomID,))
    result = cur.fetchall()
    return result

result = connect12()

def format_pandas12(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["Room"] = row[2]
        formatted["Month of Year"] = str(row[3])
        formatted["Year"] = str(row[4])
        ph = float(row[1])
        playHours = round(ph, 4)
        formatted["PlayHours"] = str(playHours)
        records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas12(result._response)
    
pandas_json = format_pandas12(result)
deltron = pd.read_json(pandas_json, orient = "records")

# In[368]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query12():
    start = datetime(2016, 8, 1)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","SerialNumber.Config.RoomName", "__day__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query12())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas12(results): # dict results
    records = []
    dictionary = {}
    for row in results.get("rows"):
        roomName = str(row.get("values")[0][1])
        s = row.get("values")[0][2]
        t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
        fmt = "%Y-%m-%d"
        day = (t.strftime(fmt)).strip().split("-")
        year = int(day[0])
        month = calendar.month_name[int(day[1])]
        day = int(day[2])
        playHours = float(row.get("values")[1])
        playHours = round(playHours, 4)
        info = (month, year, roomName)
        if (info in dictionary):
            dictionary[info] = dictionary[info] + playHours
        else:
            dictionary[info] = playHours
    for key in dictionary:
        formatted = {}
        formatted["Room"] = key[2]
        formatted["Month of Year"] = key[0]
        formatted["Year"] = key[1]
        formatted["PlayHours"] = dictionary[key]
        if (dictionary[key] != 0.0000):
            records.append(formatted)
    return json.dumps(records)
    
pandas_json = formatPandas12(result._response)

interana = pd.read_json(pandas_json,orient="records")

# Interana test for PlayHoursByCustomerByRoomByMonth
print("\n")
print("----PlayHoursByCustomerByRoomByMonth----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayHoursByCustomerByRoomByMonth
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)

# In[ ]:

def connect13():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayHoursByCustomerByRoomByWeek',(randomID,))
    result = cur.fetchall()
    return result

result = connect13()

def format_pandas13(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["RoomName"] = row[2]
        formatted["Week of Year"] = str(row[3])
        ph = float(row[1])
        playHours = round(ph, 4)
        formatted["PlayHours"] = str(playHours)
        records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas13(result._response)
    
#format_pandas(result)
pandas_json = format_pandas13(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[ ]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query13():
    start = datetime.now() - timedelta(60)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","SerialNumber.Config.RoomName", "__week__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query13())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas13(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {}
        formatted["RoomName"] = row.get("values")[0][1]
        s = row.get("values")[0][2]
        t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
        fmt = "%Y-%m-%d"
        day = (t.strftime(fmt)).strip().split("-")
        year = int(day[0])
        month = int(day[1])
        day = int(day[2])
        formatted["Week"] = date(year, month, day).isocalendar()[1]
        playHours = float(row.get("values")[1])
        playHours = round(playHours, 4)
        formatted["Playhours"] = playHours
        if (row.get("values")[1] != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    
pandas_json = formatPandas13(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayHoursByCustomerByRoomByWeek
print("\n")
print("----PlayHoursByCustomerByRoomByWeek----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayHoursByCustomerByRoomByWeek
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[ ]:

def connect14():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayHoursByCustomerByYear',(randomID,))
    result = cur.fetchall()
    return result

result = connect14()

def format_pandas14(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["Year"] = row[2]
        ph = float(row[1])
        playHours = round(ph, 4)
        formatted["PlayHours"] = str(playHours)         
    return json.dumps(records)
    pandas_json = format_pandas14(result._response)
    
#format_pandas(result)
pandas_json = format_pandas14(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[ ]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query14():
    start = datetime(2016, 8, 1)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayHours',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID", "__day__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query14())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas14(results): # dict results
    dictionary = {} 
    records = []
    formatted = {}
    for row in results.get("rows"):
        s = row.get("values")[0][1]
        t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
        fmt = "%Y-%m-%d"
        day = (t.strftime(fmt)).strip().split("-")
        year = int(day[0])
        playHours = float(row.get("values")[1])
        playHours = round(playHours, 4)
        if year in dictionary:
            dictionary[year] = dictionary[year] + playHours
        else:
            dictionary[year] = playHours
    for key in dictionary:
        formatted = {}
        formatted["Year"] = key
        formatted["PlayHours"] = dictionary[key]
        if (dictionary[key] != 0.0000):
            records.append(formatted)
    return json.dumps(records)
pandas_json = formatPandas14(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayHoursByCustomerByYear
print("\n")
print("----PlayHoursByCustomerByYear----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayHoursByCustomerByYear
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[ ]:

def connect15():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayMinutesByCustomerByHour',(randomID,))
    result = cur.fetchall()
    return result

result = connect15()

def format_pandas15(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        playMinutes = float(row[1])
        playMinutes = round(playMinutes, 4)
        formatted["PlayMinutes"] = str(playMinutes)
        formatted["Date"] = str(row[2])
        formatted["Hour"] = str(row[3])
        records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas(result._response)
    
#format_pandas(result)
pandas_json = format_pandas15(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[ ]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query15():
    start = datetime.now() - timedelta(14)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayMinutes',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID", "__day__", "__hour_of_day_in_umt__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query15())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas15(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {}
        s = row.get("values")[0][1]
        t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
        fmt = "%Y-%m-%d"
        formatted["Day"] = t.strftime(fmt)
        playMinutes = float(row.get("values")[1])
        playMinutes = round(playMinutes, 4)
        formatted["PlayMinutes"] = playMinutes
        formatted["Hour"] = row.get("values")[0][2]
        if (row.get("values")[1] != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    
pandas_json = formatPandas15(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayMinutesByCustomerByHour
print("\n")
print("----PlayMinutesByCustomerByHour----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayMinutesByCustomerByHour
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[ ]:

def connect16():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayMinutesByCustomerByRoomByDay',(randomID,))
    result = cur.fetchall()
    return result

result = connect16()

def format_pandas16(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["RoomName"] = row[2]
        formatted["Day"] = str(row[3])
        playMinutes = float(row[1])
        playMinutes = round(playMinutes, 4)
        formatted["PlayMinutes"] = str(playMinutes)
        records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas16(result._response)
    
#format_pandas(result)
pandas_json = format_pandas16(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[ ]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query16():
    start = datetime.now() - timedelta(60)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayMinutes',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","SerialNumber.Config.RoomName", "__day__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query16())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas16(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {}
        formatted["RoomName"] = row.get("values")[0][1]
        s = row.get("values")[0][2]
        t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
        fmt = "%Y-%m-%d"
        formatted["Day"] = t.strftime(fmt)
        playMinutes = float(row.get("values")[1])
        playMinutes = round(playMinutes, 4)
        formatted["PlayMinutes"] = playMinutes
        if (row.get("values")[1] != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    
pandas_json = formatPandas16(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayMinutesByCustomerByRoomByDay
print("\n")
print("----PlayMinutesByCustomerByRoomByDay----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayMinutesByCustomerByRoomByDay
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[ ]:

def connect17():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayMinutesByCustomerByRoomByHour',(randomID,))
    result = cur.fetchall()
    return result

result = connect17()

def format_pandas17(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        formatted["RoomName"] = row[2]
        formatted["Hour"] = str(row[4])
        formatted["Day"] = str(row[3])
        playMinutes = float(row[1])
        playMinutes = round(playMinutes, 4)
        formatted["PlayMinutes"] = str(playMinutes)
        records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas17(result._response)
    
pandas_json = format_pandas17(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[ ]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query17():
    start = datetime.now() - timedelta(14)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayMinutes',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","SerialNumber.Config.RoomName", "__day__","__hour_of_day_in_umt__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query17())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)

def formatPandas17(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {}
        formatted["RoomName"] = row.get("values")[0][1]
        s = row.get("values")[0][2]
        t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
        fmt = "%Y-%m-%d"
        formatted["Day"] = t.strftime(fmt)
        formatted["Hour"] = row.get("values")[0][3]
        playMinutes = float(row.get("values")[1])
        playMinutes = round(playMinutes, 4)
        formatted["PlayMinutes"] = playMinutes
        if (row.get("values")[1] != 0.0000):
            records.append(formatted)            
    return json.dumps(records)
    
pandas_json = formatPandas17(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayMinutesByCustomerByRoomByHour
print("\n")
print("----PlayMinutesByCustomerByRoomByHour----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayMinutesByCustomerByRoomByHour
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[ ]:

def connect18():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetPlayMinutesByCustomerByServiceByHour',(randomID,))
    result = cur.fetchall()
    return result

result = connect18()

def format_pandas18(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        playMinutes = float(row[1])
        playMinutes = round(playMinutes, 4)
        formatted["PlayMinutes"] = str(playMinutes)
        formatted["Service Name"] = row[2]
        formatted["Date"] = str(row[3])
        formatted["Hour"] = str(row[4])
        records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas18(result._response)
    
#format_pandas(result)
pandas_json = format_pandas18(result)
deltron = pd.read_json(pandas_json, orient = "records")


# In[ ]:

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query18():
    start = datetime.now() - timedelta(14)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='SUM',
        column='Usage.PlayMinutes',
        filter='`SonosID` in ("{}") and `Usage.MusicServiceID` not in ("2048")'.format(randomID)
    )
    query.add_params(
        max_groups=10000,
        group_by=["SonosID","Usage.MusicServiceID.serviceName", "__day__", "__hour_of_day_in_umt__"],
        sampled=False
    )
    
    #print ('Query: \n' + query.get_params())
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query18())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
    #print (results)
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas18(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {}            
        try:
            formatted["Service"] = row.get("values")[0][1]
            s = row.get("values")[0][2]
            t = datetime(1970, 1, 1) + timedelta(seconds=(s/1000.))
            fmt = "%Y-%m-%d"
            formatted["Day"] = t.strftime(fmt)
            formatted["Hour"] = row.get("values")[0][3]
            playMinutes = float(row.get("values")[1])
            playMinutes = round(playMinutes, 4)
            formatted["PlayMinutes"] = playMinutes
            if (playMinutes != 0.0000):
                records.append(formatted)       
        except IndexError:
            pass
    return json.dumps(records)
    
pandas_json = formatPandas18(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for PlayMinutesByCustomerByServiceByHour
print("\n")
print("----PlayMinutesByCustomerByServiceByHour----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for PlayMinutesByCustomerByServiceByHour
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)


# In[ ]:

def connect19():
    conn = pymysql.connect(host='deltron-production-cluster.cluster-cmjiejvhd8pl.us-east-1.rds.amazonaws.com', port=3306, user='deltron_reader', passwd='ylQo3bQ(!EL8RJ[', db='deltron_metrics', connect_timeout=5)
    cur = conn.cursor()
    cur.callproc('deltron_metrics.GetAveragePlayHoursByDayOfWeek')
    result = cur.fetchall()
    return result

result = connect19()

def format_pandas19(results): # dict results
    records = []
    for row in results:  
        formatted = {}
        dayOfWeek = str(row[0])
        playHours = float(row[1])
        playHours = round(playHours, 4)
        formatted["PlayHours"] = str(playHours)
        formatted["Day of Week"] = dayOfWeek
        records.append(formatted)            
    return json.dumps(records)
    pandas_json = format_pandas19(result._response)
    
#format_pandas(result)
pandas_json = format_pandas19(result)
deltron = pd.read_json(pandas_json, orient = "records")

HOST = 'interana.sonos.com'
TOKEN = 'zOoNAfVz4tlo0guQ6ACEEZ0IA7B5tf9kTZF7B2hmNKiX8kTYTbHdhBHHwX6BXYGyLdxg1K9C7ZEuBdGYwY9Zxu3YYmzW0000'

api_client = Client(HOST, TOKEN)
api_client._verify_certs = False

def create_query19():
    start = datetime(2016, 8, 1)
    end = datetime.now()
    query = Query('sonos_usage', start, end)
    query.add_query_info(
        type=SINGLE_MEASURE,
        aggregator='avg',
        column='Usage.PlayHours',
        filter=''
    )
    query.add_params(
        max_groups=10000,
        group_by=["day_of_week"],
        sampled=False
    )
    
    return query

# FAILS UNTIL DNS ISSUE RESOLVED
try:
    result = api_client.query(create_query19())
    results = json.dumps(result._response,sort_keys=True,indent=4, separators=(',', ': '))
except InteranaError as e:
    print(e.code, e.error)
    print(e.message)
    
def formatPandas19(results): # dict results
    records = []
    for row in results.get("rows"):
        formatted = {}            
        try:
            dayOfWeek = ""
            day = int(row.get("values")[0][0])
            if (day == 1):
                dayOfWeek = "Monday"
            elif (day == 2):
                dayOfWeek = "Tuesday"
            elif (day == 3):
                dayOfWeek = "Wednesday"
            elif (day == 4):
                dayOfWeek = "Thursday"
            elif (day == 5):
                dayOfWeek = "Friday"
            elif (day == 6):
                dayOfWeek = "Saturday"
            elif (day == 7):
                dayOfWeek = "Sunday"
            playHours = float(row.get("values")[1])
            playHours = round(playHours, 4)
            formatted["PlayHours"] = playHours
            formatted["Day of Week"] = dayOfWeek
            if (playHours != 0.0000):
                records.append(formatted)       
        except IndexError:
            pass
    return json.dumps(records)
    
pandas_json = formatPandas19(result._response)

interana = pd.read_json(pandas_json,orient="records")


# Interana test for GetAveragePlayHoursByDayOfWeek
print("\n")
print("----GetAveragePlayHoursByDayOfWeek----")
print("\n")
print("Interana: ")
print("\n")
print_full(interana)

# Deltron test for GetAveragePlayHoursByDayOfWeek
print("\n")
print("Deltron: ")
print("\n")
print_full(deltron)