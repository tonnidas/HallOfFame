import pymongo
from pymongo import MongoClient

# import csv to mongo collections
# mongoimport --type csv -d baseball -c Batting --headerline --drop Batting.csv
# mongoimport --type csv -d baseball -c candidates --headerline --drop candidates.csv
# mongoimport --type csv -d baseball -c Fielding --headerline --drop Fielding.csv
# mongoimport --type csv -d baseball -c HallOfFame --headerline --drop HallOfFame.csv

client = MongoClient()
db = client.baseball

#----------------------------------------------------------------
result = db.batting.aggregate([
    {
        '$match': {'yearID' : 1871} and {'playerID' : "abercda01"}
    }
])
for row in result:
    print(row)

for row in result:
    print(row)
#-------------------------------------------------------------------

#-------------------------------------------------------------------
# def query():
#     params = {'yearID' : 1871} and {'playerID' : "abercda01"}
#     resultset = db.batting.find(params)
#     return resultset
# res = query()
# for row in res:
#     print("\n"+row+"\n")
#--------------------------------------------------------------------

# def threek():
#     result = db.batting.aggregate([
#         {
#             '$group' : {
#                 '_id' : '$playerID',
#                 'homerun' : { '$sum' : '$HR'}
#             }
#         },
#         {
#             '$match' : { '$or' : [{'_id' : 'aaronha01'}, {'_id' :'aaronto01'}] }
#         },
#         {
#             '$sort' : {'_id' : -1}
#         }
#     ])

#     return result

# res = threek()

# for i in res:
#     print(i)


# def th():
#     result = db.fiellding.aggregate([
#         {
#             '$group' : {
#                 '_id' : '$playerID',
#                 'SS' : {
#                     '$switch' : {
#                     'branches' : [
#                         { 'case' : { '$eq' : [ 'POS', "SS" ] }, 'then' : { '$inc' : {'SS' : 1}}}  # { 'SS' : 1 }}}
#                         ]
#                     }
#                 }

#             }
#         }
#     ])

#     return result

def th():
    result = db.fielding.aggregate([
        {
            '$project' : {
                'playerID' : '$playerID', 
                'SS' : {
                    '$switch' : {
                    'branches' : [
                        { 'case' : { '$eq' : [ '$POS', "SS" ] }, 'then' : 1}  
                        ],
                    'default' : 0
                    }
                },
                '1B' : {
                    '$switch' : {
                    'branches' : [
                        { 'case' : { '$eq' : [ '$POS', "1B" ] }, 'then' : 1}  
                        ],
                    'default' : 0
                    }
                },
                '2B' : {
                    '$switch' : {
                    'branches' : [
                        { 'case' : { '$eq' : [ '$POS', "2B" ] }, 'then' : 1}  
                        ],
                    'default' : 0
                    }
                },
                '3B' : {
                    '$switch' : {
                    'branches' : [
                        { 'case' : { '$eq' : [ '$POS', "3B" ] }, 'then' : 1}  
                        ],
                    'default' : 0
                    }
                },
                'OF' : {
                    '$switch' : {
                    'branches' : [
                        { 'case' : { '$eq' : [ '$POS', "OF" ] }, 'then' : 1}  
                        ],
                    'default' : 0
                    }
                },
                'C' : {
                    '$switch' : {
                    'branches' : [
                        { 'case' : { '$eq' : [ '$POS', "C" ] }, 'then' : 1}  
                        ],
                    'default' : 0
                    }
                },
                'P' : {
                    '$switch' : {
                    'branches' : [
                        { 'case' : { '$eq' : [ '$POS', "P" ] }, 'then' : 1}  
                        ],
                    'default' : 0
                    }
                }
            }
        }
    ])
    return result

# result = db.fielding.aggregate([
#     {
#         "$group": {
#         "_id": {
#             "playerID": "$playerID",
#             "pos": "$POS"
#         },
#         "count": {
#             "$sum": 1
#         }
#         }
#     },
#     {
#         "$sort": {
#             "_id.playerID": 1,
#             "count": -1
#         }
#     },
#     {
#         "$group": {
#             "_id": '$_id.playerID',
#             "pos": {
#                 "$first": "$_id.pos"
#             }
#         }
#     }
# ])

# for i in result:
#     print(i)
