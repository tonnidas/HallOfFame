import pymongo
from pymongo import MongoClient

# Connect to Mongo database
client = MongoClient()
db = client.baseball

outputFile = 'jui.mongo'

# ------------------------------------------- Begin Functions -------------------------------------------

# To get players information of games played, at bats, runs scored, hits, doubles, triples, home runs, RBI, walks, strikeouts, stolen bases from BATTING.CSV
def getScore(table):
    result = table.aggregate([ 
        {
            '$group' : {
                '_id' : '$playerID',
                'G' : {'$sum' : '$G'},
                'AB' : {'$sum' : '$AB'},
                'R' : {'$sum' : '$R'},
                'H' : {'$sum' : '$H'},
                '2B' : {'$sum' : '$2B'},
                '3B' : {'$sum' : '$3B'},
                'HR' : {'$sum' : '$HR'},
                'RBI' : {'$sum' : '$RBI'},       
                'BB' : {'$sum' : '$BB'},
                'SO' : {'$sum' : '$SO'},
                'SB' : {'$sum' : '$SB'},
            }
        },
    ])

    # Converting into dictionary list so that it later can be used for less computation cost
    scoreList = list()
    for i in result: 
        scoreList.append(i)

    return scoreList

# Extracts primary position from fielding file
def pos():
    result = db.fielding.aggregate([
    {
        # Grouping with a composite key
        "$group": {
        "_id": {
            "playerID": "$playerID",
            "pos": "$POS"
        },
        # Summing total key value pairs: {key : value} = {(playerID,pos) : count}
        "count": {
            "$sum": 1
        }
        }
    },
    {
        # Sorting in ascending order for the playerID, 
        # and when there is same playerID for multiple, sorting among them according to count in descending order
        # {(a,SS) : 5}, {(a,2B) : 3}, {(b, 3B) : 4}
        "$sort": {
            "_id.playerID": 1,
            "count": -1
        }
    },
    {
        # Making a key-value pair for playerID and the first position in the sorted order: {key : value} = {playerID : position} = {a : SS}, {b : 3B}
        "$group": {
            "_id": '$_id.playerID',
            "pos": {
                "$first": "$_id.pos"
            }
        }
    }
    ])

    # ignore pitcher
    posDict = {}

    for r in result:
        pid = r['_id']

        if(r['pos'] == "SS"):
            posDict[pid] = 168
        elif(r['pos'] == "1B"):
            posDict[pid] = 12
        elif(r['pos'] == "2B"):
            posDict[pid] = 132
        elif(r['pos'] == "3B"):
            posDict[pid] = 84
        elif(r['pos'] == "OF"):
            posDict[pid] = 48
        elif(r['pos'] == "C"):
            posDict[pid] = 240
            
    return posDict

# To get the names of people who are in the hall of fame
def hallOfFame():
    result = db.halloffame.aggregate([
        {
            '$match' : { 'inducted' : 'Y' }
        },
        {
            '$project' :  {'_id' : '$playerID'}
        }
    ])

    hofList = list()

    for i in result:
        hofList.append(i['_id'])

    return hofList

# find similarity of two palyers
def getSimilarity(x, y, px, py):
    a = (20, 75, 10, 15, 5, 4, 2, 10, 25, 150, 20, 0.001, 0.002)

    similarity = 1000
    for i in range(13):
        similarity = similarity - (abs(x[i] - y[i]) / a[i])

    # subtract position difference
    similarity = similarity - abs(px-py)
    return similarity

# Calculating players batting avg
def getBattingAvg(c):
    c_bavg = 0
    if(c['AB'] != 0):
        c_bavg = c['H'] / c['AB']
    return c_bavg

# Calculating players slugging percentage
def getSluggingPtg(c):
    c_sp = 0
    if(c['AB'] != 0):
        c_sp = (c['H'] + c['2B'] + 2*c['3B'] + 3*c['HR']) / c['AB']
    return c_sp

# ------------------------------------------- End Functions -------------------------------------------

# Getting all the MongoDB dictionary pairs in dataframes
battingData = getScore(db.batting)
candidateData = getScore(db.candidates)
posData = pos()
hofData = hallOfFame()

# To store the final answer in this list
ansList = list()

# for each candidate, calculate similarity between all other players in the batting file  // x = candidate, y = batter
for c in candidateData:

    # Ignore the pitcher candidates
    if c['_id'] not in posData:
        continue

    simList = list()
    for b in battingData:

        # Ignore the pitcher batters
        if b['_id'] not in posData:
            continue

        # To get the position value from posData
        px = posData[c['_id']]
        py = posData[b['_id']]

        # Do not calculate similarity with itself
        if(c['_id'] != b['_id']):
            
            # Retrieving candidates batting avg and slugging percentage
            c_bavg = getBattingAvg(c)
            c_sp = getSluggingPtg(c)

            # Retrieving batters batting avg and slugging percentage
            b_bavg = getBattingAvg(b)
            b_sp = getSluggingPtg(b)

            # Making the candidate and batter array to send them to getSimilarity function
            x = (c['G'], c['AB'], c['R'], c['H'], c['2B'], c['3B'], c['HR'], c['RBI'], c['BB'], c['SO'], c['SB'], c_bavg, c_sp)
            y = (b['G'], b['AB'], b['R'], b['H'], b['2B'], b['3B'], b['HR'], b['RBI'], b['BB'], b['SO'], b['SB'], b_bavg, b_sp)

            sim = getSimilarity(x, y, px, py)
            simList.append((sim, b['_id']))

    # sort to get maximum similarity with b for each c
    sortedSim = sorted(simList, reverse = True, key=lambda x:x[0])

    # take 5 maximum similarity and count how many of them are in HOF
    count = 0
    for i in range(5):
        # for debugging
        # if c['_id'] == 'bondsba01':
        #     print(sortedSim[i][0], sortedSim[i][1])

        if sortedSim[i][1] in hofData:
            count += 1

    # check at least 3 of them are in the hall of fame
    if(count >= 3):
        ansList.append(c['_id'])

print("List of players that should be in the Hall Of Fame")
print(ansList)

# save output
with open(outputFile, 'w') as f:
    for item in ansList:
        f.write("%s\n" % item)
