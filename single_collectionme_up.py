# -*- coding: utf-8 -*-
"""
Created on Fri Dec 25 11:42:16 2020

@author: Hp
"""
import logging 
import pandas as pd
import numpy as np
import requests
from pymongo import MongoClient
from multiprocessing import  Pool,Manager
import sys
import concurrent.futures
from concurrent.futures import wait, ALL_COMPLETED
from collections import defaultdict
from time import time
import json
from collections import defaultdict
from geopy.geocoders import Photon
from datetime import datetime
import os , binascii  #for genrating resrauntId hex
import math
from datetime import datetime
api_key='zqVXmSzDDGY7pCmoLAP6h83vAYPHmU8Id9gXqFOeaB9NaxW8tdOpD1mjoSlwGfCfe31A9WfsV1My9ekOajXqc1KnzCO4M6Zn3JXkss1-Yzw_Pmw6h54hmsxO6CkUX3Yx'
head = {'Authorization': 'Bearer %s' % api_key}
yelpUrl='https://api.yelp.com/v3/businesses/matches'

rId=0
countriesDict = []  
countriesVybeDict = []
errorRecordDict = []
 
iterationStatusCode = {'0':0,'1':0,'2':0,'3':0, '4': 0}


presentTime = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
logName = "Buffalo"+presentTime+".log"
#logging.basicConfig(filename=logName,filemode='w', encoding='utf-8', level=logging.INFO, format="( %(asctime)s ) - %(levelname)s - %(message)s")
root_logger= logging.getLogger()
root_logger.setLevel(logging.INFO) # or whatever
handler = logging.FileHandler(logName, 'w', 'utf-8') # or whatever
handler.setFormatter(logging.Formatter("( %(asctime)s ) - %(levelname)s - %(message)s")) # or whatever
root_logger.addHandler(handler)

with open(sys.argv[1],'r') as f:
    fileName = f.readline().strip()
    client = MongoClient(f.readline().strip())
    db = client[f.readline().strip()]
    errorExcelSheetName = f.readline().strip()
    stateName = f.readline().strip()

df = pd.read_excel(file_name)
df.rename(columns = {'Category':'cuisine', 'Address': 'oldAddress' , 'Name' : 'name', 'Area': 'area', 'Price': 'priceRange', 'Phone' : 'phone'}, inplace = True) 
df['name'] = df['name'].str.strip()

df.drop(['cols111','cols116','Timings'], axis = 1,inplace=True) 

def genRows(df):
    for row in df.itertuples(index=False):
        yield row._asdict()
def stripAndSplit(stringValue):
    # function to split on comma and strip each element 
    try:
        return [str(eachString).strip() for eachString in str(stringValue).split(",") ]
    except:
        root_logger.warning("Error in stripAndSplit function")
        return None

    
def parseTime(timeList):
    try:
        updatedtimeList = []
        for eachObj in timeList:
            isOvernight = eachObj['open'] >= eachObj['close']
            updatedtimeList.append({ 'day': eachObj['day'] , 'start': eachObj['open'] , 'end': eachObj['close'] ,'isOvernight' : isOvernight})
        return updatedtimeList
    except:
        root_logger.warning("Error in timeList function")
        return None
def parseVybe(vybeDataList) :
    try:
        if len(vybeDataList) == 0:
            return None
        vybe = {}
        for eachObj in vybeDataList :
            vybe[eachObj["name"]] = eachObj["data"]
        return vybe
    except:
        root_logger.warning("Error in vybeDataList function")
        return None

def parseAddress(brokenAddressList):
    try:
        if brokenAddressList is None or not(len(brokenAddressList)):
            return None
        fullAddress = {'landMark':brokenAddressList[0],
                    'streetAddress' : brokenAddressList[1],
                    'city' : brokenAddressList[3],
                    'zipcode' : brokenAddressList[4],
                    'state' : brokenAddressList[5],
                    'country' :  brokenAddressList[6] 
                    }
        return fullAddress
    except:
        root_logger.warning("Error in brokenAddressList function")

def updateRow(row):
    vybeRow = {}
    global countriesDict, iterationStatusCode, errorRecordDict ,rId
    row['cuisine'] = stripAndSplit(row['cuisine'])
    row['phone'] = stripAndSplit(row['phone'])
    row['__v'] = 0
    row['rating'] = None
    row['tags'] = None
    row['videoLink'] = None
    row['reviewCount'] = None
    row['isClaimed'] = None
    row['faq'] = None
    row['category'] = None
    row['securityDocumentUrl'] = None
    row['securityDocumentName'] = None
    row['socialMediaHandles'] = { "facebook": None, "instagram": None, "twitter": None, "snapchat": None, "pinterest": None }
    row['hoursType'] = "REGULAR"
    row['isOpenNow'] = True
    row['specialHours'] = [{ "date": None, "start": None, "end": None , "isOvernight": None }]
    row['attributes'] = { "businessParking": { "garage": None, "street": None} ,"genderNeutralRestrooms": None, "openToAll": None, "restaurantsTakeOut": None, "wheelChairAccessible": None }

    postRestaurantName = row['name']
    postAddress = row['oldAddress']
    postCity = row["area"].split(",")[-1]
    url = "https://qozddvvl55.execute-api.us-east-1.amazonaws.com/Prod/vybe/"
    payload = {
                "name": postRestaurantName,
                "address": postAddress
                }
    headers = { 'Content-Type': 'text/plain' }

    for i in range(5):
        if i == 1 :
            payload['address'] = postCity
        elif i== 2:
            payload['address'] = postAddress.split(",")[-1]
        elif i== 3:
            params = {'name':postRestaurantName,"city":postCity,'address1':postAddress,'state':stateName,'country':"US"}
            yelpReq = requests.get(yelpUrl, params=params, headers=head)
            try:
                address = " ".join(yelpReq.json()["businesses"][0]["location"]["display_address"])
                payload['address'] = address
            except:
                root_logger.warning(f"Yelp can't find the address for {payload['name']}")
                continue
        elif i ==4:
            #insert code here for photon tomtom or anything
            try:
                geolocator = Photon(user_agent = "My-app")
                location = geolocator.geocode(postAddress)
                if location is not None:
                    payload['address'] = location.address
                else :
                    root_logger.warning(f"Photon could not find address for {payload['name']}")
            except Exception as e:
                root_logger.warning(f"Photon failed for {payload['name']}")
                root_logger.exception(e)
        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
        statusCode = response.status_code
        root_logger.info(f"{statusCode} - ({postRestaurantName}) - ({payload['address']}) iteration ({i})")
        if statusCode == 200:
            iterationStatusCode[str(i)] += 1
        if statusCode ==200 or statusCode == 502:
            break   
    
  
    if statusCode == 200 :
        res = response.json().get('message') # moving this line outside makes code not update documents without vybe data
        '''Res_Ids=list(map(itemgetter("restaurantId"),countriesDict))
        while(True):
            lst = [random.choice(string.ascii_letters + string.digits) for n in range(10)]
            restaurantId = 'RestId'+"".join(lst)
            if restaurantId not in Res_Ids:
                break'''
        restaurantId = 'RestId' + datetime.now().strftime("%d%m%y%H%M") + str(rId).zfill(3)
        rId = (rId +1)%1000    #increase this if want to use threading to execute more than 1000 records at a time, but not needed right now
        #verify id
        hour = parseTime(res.get('timings'))
        website = res.get('website')
        address = parseAddress(res.get('address_broken'))
        coordinates = { "coordinates": [res.get('coordinates').get("longitude"), res.get('coordinates').get("latitude") ], "type" : "Point" }
        newAddress = res.get('address')
        
        vybe  = parseVybe(res.get('vybe'))
        
        if vybe:
            
            minTimeSpent =  res.get('timeSpent')[0] if  res.get('timeSpent') else None
            maxTimeSpent =  res.get('timeSpent')[1] if  res.get('timeSpent') else None
            vybeRow.update({"restaurantId" : restaurantId, 'vybe' : vybe, 'minTimeSpent' : minTimeSpent , 'maxTimeSpent': maxTimeSpent })
            countriesVybeDict.append(vybeRow)
        else:
            root_logger.warning("{} No VybeData ( {} ) ( {} )".format(statusCode,postRestaurantName,postAddress))
            
        row.update({"restaurantId" : restaurantId,"location": coordinates ,"newAddress": newAddress ,"hours" : hour, "website": website, "address": address })
    
        countriesDict.append(row)
        #print(countriesDict)
    
     
    else :
        hour = None
        website = None
        address = None
        newAddress = None
        address = None
        row.update({"newAddress": newAddress ,"hours" : hour, "website": website, "address": address })
        errorRecordDict.append(row)
        root_logger.info("{} Entered retraunt with name ( {} ) and address( {} ) into error file".format(statusCode,postRestaurantName,postAddress))
    return statusCode

if __name__ == "__main__":     
    row = genRows(df)   
    k=0
    numOfThreads = int(sys.argv[2])
    threadBatch = 1
    totalExcelRows = len(df.index)
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        #while numOfThreads*threadBatch <= round(totalExcelRows/numOfThreads)*numOfThreads:
        for i in range(math.ceil(totalExcelRows/numOfThreads)):
            if totalExcelRows>numOfThreads:
                k=numOfThreads
                totalExcelRows=totalExcelRows-numOfThreads
            else:
                k=totalExcelRows
            try:
                apiStatusCode = defaultdict(int)
                startTimeForOneBatch = time()
                future =[ executor.submit(updateRow,next(row)) for i in range(k)]
                print("Threads Over - {}".format(len(future)))
                for thread in future:
                    try:
                        statusCode = thread.result()
                        apiStatusCode[statusCode] += 1
                    except StopIteration as e:
                        break
                    except Exception as e:
                        root_logger.exception(e)
                        continue
                for statusCodeKey,countValue in apiStatusCode.items():
                    root_logger.warning("THREADs -{} STATUS CODE -{}".format(countValue,statusCodeKey))
                    print("THREADs -{} STATUS CODE -{}".format(countValue,statusCodeKey))
                threadBatch += 1
            except StopIteration as e:
                break
            except Exception as e:
                root_logger.exception(e)
                if(concurrent.futures.wait(future, timeout=None, return_when=ALL_COMPLETED)):
                    break
            timeTakenToFinishOneBatch = time()
            print("time = {} taken for thread batch = {}".format(timeTakenToFinishOneBatch - startTimeForOneBatch,threadBatch-1))
        
    try:
        collectionName = "California1"  #default need to know what to do with this
        collectionToBeInserted = db[collectionName]
        collectionToBeInserted.insert_many(countriesDict)
        print("Created Collection ",collectionName)
        root_logger.info("Created Collection {}".format(collectionName))
    except Exception as e:
        root_logger.warning(e)

    try:
        collectionName = collectionName +"Vybe"
        collectionToBeInserted = db[collectionName]
        collectionToBeInserted.insert_many(countriesVybeDict)
        print("Created Collection ",collectionName)
        root_logger.info("Created Collection {}".format(collectionName))
    except Exception as e:
        root_logger.warning(e)
    
    errorExcelDataFrame = pd.DataFrame.from_dict(errorRecordDict)
    errorExcelDataFrame.to_excel(errorExcelSheetName+".xlsx")
