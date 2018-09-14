# encoding=utf8

import sys
import os
reload(sys)
sys.setdefaultencoding('utf8')

import csv
from datetime import datetime, timedelta
from os import listdir
from os.path import isfile, join
from pymongo import MongoClient

client = MongoClient()
db = client.ClimateChangeTrack_576d402fe165d43c30c669cf

outfile = './Quoted_Tweet_Climate_Change.csv'
csvfile = open(outfile, 'w')
writer = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)
writer.writerow(['Quoted_tweet_id','Quoted_tweet_created_at','Quoted_tweet_hashtags','Quoted_media_type','Quoted_user_mentions','Quoted_url','Quoted_user_screen_name','Quoted_user_created_at','Quoted_user_follower_count','Quoted_user_friends_count','Quoted_user_status_count','Quoted_status_text','tweet_id','tweet_created_at','user_screen_name','user_followers_count','user_status_count','tweet_text'])

with open('tweet_id.csv','r') as csvfile:
    reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in reader:
       r=','.join(row)
       x=r.split('_')
       print(x)
       tweets=db.tweets.find({"quoted_status.id_str":x[1]})
       y=tweets.count()
       print(y)
       if(y>0):
          for tweet in tweets:
             Quoted_tweet_id=""
             Quoted_tweet_created_at=""
             Quoted_tweet_hashtags=""
             Quoted_media_type=""
             Quoted_user_mentions=""
             Quoted_urls=""
             Quoted_user_screen_name=""
             
             Quoted_user_created_at=""
             Quoted_status_text=""
             Quoted_tweet_id ="ID_"+ str(tweet['quoted_status']['id_str'])
             Quoted_tweet_created_at=tweet['quoted_status']['created_at']

             hashtag=[]
             if "hashtags" in tweet['quoted_status']['entities']:
                hashtag_num = len(tweet['quoted_status']['entities']['hashtags'])
                for index in range(len(tweet['quoted_status']['entities']['hashtags'])):
                   hashtag.append(tweet['quoted_status']['entities']['hashtags'][index]['text'].lower())
             hashtags='|'.join(hashtag)

             media=[]
             if "media" in tweet['quoted_status']['entities']:
                media_num = len(tweet['quoted_status']['entities']['media'])
                for index in range(len(tweet['quoted_status']['entities']['media'])):
                   media.append(tweet['quoted_status']['entities']['media'][index]['type'].lower())
             medias='|'.join(media)

             user_mentions=[]
             if "user_mentions" in tweet['quoted_status']['entities']:
                mentions_num = len(tweet['quoted_status']['entities']['user_mentions'])
                for index in range(len(tweet['quoted_status']['entities']['user_mentions'])):
                   user_mentions.append(tweet['quoted_status']['entities']['user_mentions'][index]['name'].lower())
             mentions='|'.join(user_mentions)

             urls=[]
             if "media" in tweet['quoted_status']['entities']:
                media_num = len(tweet['quoted_status']['entities']['media'])
                for index in range(len(tweet['quoted_status']['entities']['media'])):
                   urls.append(tweet['quoted_status']['entities']['media'][index]['url'].lower())
             url='|'.join(urls)

             Quoted_user_screen_name=tweet['quoted_status']['user']['screen_name']
             Quoted_user_created_at=tweet['quoted_status']['user']['created_at']
             Quoted_user_follower_count=tweet['quoted_status']['user']['followers_count']
             Quoted_user_friends_count=tweet['quoted_status']['user']['friends_count']
             Quoted_user_status_count=tweet['quoted_status']['user']['statuses_count']
             Quoted_status_text=tweet['quoted_status']['text']
 
             Original_tweet_id='ID_'+tweet['id_str']
             Original_tweet_created_at=tweet['created_at']
             Original_user_screen_name=tweet['user']['screen_name']
             Original_user_follower_count=tweet['user']['followers_count']
             Original_user_statuses_count=tweet['user']['statuses_count']
             Original_tweet_text=tweet['text']

             writer.writerow([Quoted_tweet_id,Quoted_tweet_created_at,hashtags,medias,mentions,url,Quoted_user_screen_name,Quoted_user_created_at,Quoted_user_follower_count,Quoted_user_friends_count,Quoted_user_status_count,Quoted_status_text,Original_tweet_id, Original_tweet_created_at, Original_user_screen_name,Original_user_follower_count,Original_user_statuses_count, Original_tweet_text])

