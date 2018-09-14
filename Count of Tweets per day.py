import sys
reload(sys)
sys.setdefaultencoding('utf8')

import csv
from datetime import datetime, timedelta
from os import listdir
from os.path import isfile, join
from pymongo import MongoClient

client = MongoClient()
db = client.ClimateChangeTrack_576d402fe165d43c30c669cf 

# Define start and end dates: yyyy, m, d, h
start_date = datetime(2016, 9, 1, 0)
end_date = datetime(2017, 8, 31, 0)

outfile = './ClimateChangeTrack_no_of_tweets.csv'
csvfile = open(outfile, 'w')
writer = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)
writer.writerow(['tweet_date','tweet_count'])

tweets=db.tweets.aggregate(
       [
         {
           "$group":
             {
               "_id": {
                    "day": { "$dayOfMonth": "$created_ts"},
                    "month" : {"$month" : "$created_ts" },
                    "year": { "$year": "$created_ts" },
                    },
               "TotalTweetsPerDay": { "$sum": 1 },
             }
         },
         {"$sort" : {" _id ": -1}}
       ]
)

for tweet in tweets['result']:
    tweet_date = tweet['_id']

    tweet_date = "%02d/%02d/%04d" % (tweet_date['month'],tweet_date['day'],tweet_date['year'])
    tweet_count = tweet['TotalTweetsPerDay']
    tweet_dte = datetime.strptime(tweet_date,'%m/%d/%Y')
    if(tweet_dte>=start_date and tweet_dte <= end_date):
        writer.writerow([tweet_date,tweet_count])


