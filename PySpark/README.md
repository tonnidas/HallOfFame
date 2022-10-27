# Cloud Computing
# Assignment 03 - Who Should be in the Hall of Fame?

## **Script**
- `hof.py` Defines a spark program to determine similarity between two baseball players of same career state attributes using baseball reference formula.

    Input and output description:

    * It takes into account 3 csv files in total namely, ``Batting.csv``, ``HallOfFame.csv`` and `Fielding.csv`.
    * It also takes into account an input HDFS file from `candidates.csv`. The program checks using similarity formula if the player from this input files should be in the `HallOfFame.csv` file  but are not. 
    * As output, all the players names from `candidates.csv` file will be stored in the `"/user/jui"` HDFS file location.


## **Functions**
- `getInt` Defines a function to convert string into integer, returns zero if empty.
- `notNone` Defines a function to filter out none rows for example, when the key of a mapper function is `playerId`, it is returning none as I do not want the first row of any csv file. So `notNone` function will filter out this kind of rows.
- `mapperBatting` Defines a map function to take the only necessary information from a csv file in the form of a tuple (as for a few players whose names appeared only once, they do not go to reducer because nothing to reduce there, so they go to the RDD directly as a tuple).
- `reduceBatting` Defines a reduce function to sum up all information for the same player and keeps summing up and return a tuple of same size of `mapperBatting`.
- `mapperAvg` Defines a map function to calculate the batting average and slugging percentage any player.
- `mapperPos` Defines a map function to extract position of any player from fielding file.
- `reduceSumPos` Defines a reduce function to count number of different positions a player has played.
- `mapperFinalPos` Defines a map function to get the primary position of a player.
- `getSimilarity` Defines a function to find similarity of two palyers using the similarity checking formula.
- `mapperHOF` Defines a map function to extract playerIDs from HallOfFame so that later we can match if a player is in the hall Of Fame.


## **RDDs**
- `posRDD` : Holds the position value of each player from the `Fielding.csv` file.
- `battingRDD` : It holds all the necessary information from `Batting.csv` file for each player in one row.
- `candidateRDD` : It holds all the necessary information from `candidates.csv` file for each player in one row.

## **Lists**
- `bInfo` : It holds all information for each player including position by joining position with batting players.
- `cInfo` : It holds all information for each player including position by joining position with candidates players.
- `hofList` : It holds list of players in HallOfFame.
- `ansList` : stores the players who should be in the `hallOfFame.csv` but are not.

## **How to run**
- Make sure to upload a `candidates.csv` file in `data` folder in hadoop so that program can find its input from there.

    * The candidate file should have players who are not in the `HallOfFame.csv` file.
    * The candidate file should have all the rows for any player in it.
- View the output players names in `hdfs -dfs -cat /user/jui/*`.
