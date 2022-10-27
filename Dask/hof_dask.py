# Author: Tonni Das Jui

import dask.bag as db
import dask.dataframe as dd
from collections import defaultdict
import operator

dataBatting = db.read_text("Batting.csv")
dataFielding = db.read_text("Fielding.csv")
dataHOF = db.read_text("HallOfFame.csv")
dataCandidate = db.read_text("candidatesB.csv")
outputFile = "jui.dask"

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
        
        value = [getInt(l[5]), getInt(l[6]), getInt(l[7]), getInt(l[8]), getInt(l[9]), getInt(l[10]), 
                getInt(l[11]), getInt(l[12]), getInt(l[15]), getInt(l[16]), getInt(l[13]), 0, 0]

        return {key: value}

# sums data of batting/candidate for a specific palyerID
def reduceBatting(seq):
	reduced = defaultdict()

	for x in seq:
		for k,v in x.items():
			if k in reduced:
				for i in range(13):
					reduced[k][i] = reduced[k][i] + v[i]
			else:
				reduced[k] = v
	
	# find batting avg and slugging percentage
	for k,v in reduced.items():
		if(v[1] != 0):
			reduced[k][11] = v[3]/v[1]
			reduced[k][12] = (v[3] + v[4] + 2*v[5] + 3*v[6])/v[1]

	return reduced

# extracts position from fielding file
def mapperPos(s):
    l = s.split(",")
    key = l[0]
    value = [0, 0, 0, 0, 0, 0, 0]
    pos = ["SS", "1B", "2B", "3B", "OF", "C", "P"]

    if l[0] != "playerID":
        idx = pos.index(l[5])
        value[idx] = 1
        return {key: value}

# counts number of different positions a player has played
def reduceSumPos(seq):
    reduced = defaultdict()

    for x in seq:
        for k,v in x.items():
            if k in reduced:
                for i in range(7):
                    reduced[k][i] = reduced[k][i] + v[i]
            else:
                reduced[k] = v
	
	# assign value of the primary position
    # use 0 for primary POS = 'P'
    pos = [168, 12, 132, 84, 48, 240, 0]

    for k,v in reduced.items():
        if type(v) == list:
            max_index, max_value = max(enumerate(v), key=operator.itemgetter(1))
            reduced[k] = pos[max_index]

    return reduced

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
# check if inducted is Y
def mapperHOF(s):
    l = s.split(",")
    key = l[0]
    if(l[0] != "playerID" and l[6] == 'Y'):
        return key

if __name__ == '__main__':
	
    battingDict = dataBatting.map(mapperBatting).filter(notNone).reduction(reduceBatting, reduceBatting).compute()
    candidateDict = dataCandidate.map(mapperBatting).filter(notNone).reduction(reduceBatting, reduceBatting).compute()
    posDict = dataFielding.map(mapperPos).filter(notNone).reduction(reduceSumPos, reduceSumPos).compute()
    hofList = dataHOF.map(mapperHOF).filter(notNone).compute()
    
    # for debugging five most similar to bondsba01
    candidateDict = {'ewingbu01': battingDict['ewingbu01']}

    # # for debugging five most similar to troutmi01
    # candidateDict = {'troutmi01': battingDict['troutmi01']}

    ansList = list()

    # for each candidate, calculate similarity between all other players in the batting file
    for ck, cv in candidateDict.items():
        # to make sure player exists in Fielding file
        # ignore if primary position is 'P'
        if ck not in posDict or posDict[ck] == 0:
            continue

        simList = list()

        for bk, bv in battingDict.items():
            # do not calculate similarity with itself
            # ignore if primary position is 'P'
            if(ck != bk and bk in posDict and posDict[bk] != 0):
                sim = getSimilarity(cv, bv, posDict[ck], posDict[bk])
                simList.append((sim, bk))
        
        # sort to get maximum similarities
        sortedSim = sorted(simList, reverse = True, key=lambda x:x[0])

        # take 5 maximum similarity and count how many of them are in HOF
        count = 0
        for i in range(5):
            if sortedSim[i][1] in hofList:
                count += 1

        # For debugging purpose
        count = 0
        for i in range(5):
            print("Similarity and simname", sortedSim[i][0], sortedSim[i][1])
            if sortedSim[i][1] in hofList:
                count += 1
                
        # For debugging purpose      
        count = 0
        for i in range(5):
            if sortedSim[i][1] in hofList:       
                print("simName who are in halloffame = ",sortedSim[i][1])

        # top 5 similar Batting players list for each candidate player
        # print(ck, count, sortedSim[0], sortedSim[1], sortedSim[2], sortedSim[3], sortedSim[4])
        
        # check at least 3 of them are in the hall of fame
        if(count >= 3):
            ansList.append(ck)

    print("List of players that should be in the Hall Of Fame")
    print(ansList)

    # save output
    with open(outputFile, 'w') as f:
        for item in ansList:
            f.write("%s\n" % item)
