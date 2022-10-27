
# Author: Tonni Das Jui

import pyspark
from pyspark import SparkContext,  SparkConf
import operator

conf = SparkConf().setAppName('hof').setMaster('local')
sc = pyspark.SparkContext(conf=conf)

dataBatting = sc.textFile("Batting.csv")
dataFielding = sc.textFile("Fielding.csv")
dataHOF = sc.textFile("HallOfFame.csv")
dataCandidate = sc.textFile("candidates1.csv")
outputFile = "hdfs://localhost:9820/user/jui"

# convert string into integer, returns zero if empty
def getInt(x):
    if(x == ''):
        return 0
    else:
        return int(x)

# filters out none rows
# e.g header rows
def notNone(kv):
    return kv is not None

# extract necessary data from batting/candidate files
# key is playerID
def mapperBatting(s):
    l = s.split(",")
    key = l[0]
    
    if(l[0] != "playerID"):
        # games played, at bats, runs scored, hits, doubles, triples, home runs, RBI, walks, strikeouts, stolen bases, batting average, slugging percentage
        
        value = (getInt(l[5]), getInt(l[6]), getInt(l[7]), getInt(l[8]), getInt(l[9]), getInt(l[10]), 
                getInt(l[11]), getInt(l[12]), getInt(l[15]), getInt(l[16]), getInt(l[13]), 0, 0)

        return key, value

# sums data of batting/candidate for a specific palyerID
def reduceBatting(x,y):
    if(type(x) == tuple):
        arr = list()
        for i in range(13):
            temp = x[i] + y[i]
            arr.append(temp)
        return tuple(arr)
    return y

# find batting avg and slugging percentage
def mapperAvg(s):
    key = s[0]
    value = list(s[1])

    if(value[1] != 0):
        value[11] = value[3]/value[1]
        value[12] = (value[3] + value[4] + 2*value[5] + 3*value[6])/value[1]

    return key, tuple(value)

# extracts position from fielding file
def mapperPos(s):
    l = s.split(",")
    key = l[0]
    value = (0, 0, 0, 0, 0, 0)
    if(l[0] != "playerID"):
        if(l[5] == "SS"):
            value = (1, 0, 0, 0, 0, 0)
        if(l[5] == "1B"):
            value = (0, 1, 0, 0, 0, 0)
        if(l[5] == "2B"):
            value = (0, 0, 1, 0, 0, 0)
        if(l[5] == "3B"):
            value = (0, 0, 0, 1, 0, 0)
        if(l[5] == "OF"):
            value = (0, 0, 0, 0, 1, 0)
        if(l[5] == "C"):
            value = (0, 0, 0, 0, 0, 1)
        return key, value

# counts number of different positions a player has played
def reduceSumPos(x,y):
    if(type(x) == tuple):
        arr = list()
        for i in range(len(y)):
            temp = x[i] + y[i]
            arr.append(temp)
        return tuple(arr)
    return y

# get the primary position of a player
def mapperFinalPos(s):
    key = s[0]
    value = list(s[1])
    pos = 0
    max_index, max_value = max(enumerate(value), key=operator.itemgetter(1))
    if(max_index == 0):
        pos = 168
    elif(max_index == 1):
        pos = 12
    elif(max_index == 2):
        pos = 132
    elif(max_index == 3):
        pos = 84
    elif(max_index == 4):
        pos = 48
    elif(max_index == 5):
        pos = 240
    return key, pos

# find similarity of two palyers
def getSimilarity(x, y, px, py):
    a = (20, 75, 10, 15, 5, 4, 2, 10, 25, 150, 20, 0.001, 0.002)

    similarity = 1000
    for i in range(13):
        similarity = similarity - (abs(x[i] - y[i]) / a[i])

    # subtract position difference
    similarity = similarity - abs(px-py)
    return similarity

# extracts playerIDs from HallOfFame
def mapperHOF(s):
    l = s.split(",")

    # i added this part
    key = None
    if(l[0] == "Y"):
        key = l[0]
    
    if(l[0] != "playerID"):
        return key


posRDD = dataFielding.map(mapperPos).filter(notNone).reduceByKey(reduceSumPos).map(mapperFinalPos)
battingRDD = dataBatting.map(mapperBatting).filter(notNone).reduceByKey(reduceBatting).map(mapperAvg)
candidateRDD = dataCandidate.map(mapperBatting).filter(notNone).reduceByKey(reduceBatting).map(mapperAvg)

# join position with batting and candidate
bInfo = battingRDD.join(posRDD).collect()
cInfo = candidateRDD.join(posRDD).collect()

# list of players in HOF
hofList = dataHOF.map(mapperHOF).filter(notNone).collect()

# for debugging five most similar to Barry Bonds in HOF
# cInfo = candidateRDD.join(posRDD).filter(lambda x: x[0] == 'bondsba01').collect()
# bInfo = battingRDD.join(posRDD).filter(lambda x: x[0] in hofList).collect()

ansList = list()

# for each candidate, calculate similarity between all other players in the batting file
for c in cInfo:
    simList = list()

    for b in bInfo:
        # do not calculate similarity with itself
        if(c[0] != b[0]):
            sim = getSimilarity(c[1][0], b[1][0], c[1][1], b[1][1])
            simList.append((sim, b[0]))
    
    # sort to get maximum similarity with b for each c
    sortedSim = sorted(simList, reverse = True, key=lambda x:x[0])

    # top 5 similar Batting players list for each candidate player
    # print(c[0], sortedSim[0], sortedSim[1], sortedSim[2], sortedSim[3], sortedSim[4])

    # take 5 maximum similarity and count how many of them are in HOF
    count = 0
    for i in range(5):
        if sortedSim[i][1] in hofList:
            count += 1

    # check at least 3 of them are in the hall of fame
    if(count >= 3):
        ansList.append(c[0])

print("List of players that should be in the Hall Of Fame")
print(ansList)

# save output to HDFS
sc.parallelize(ansList).saveAsTextFile(outputFile)

# view output
# hdfs -dfs -cat /user/jui/*
