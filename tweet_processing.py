import json
import sys
import hashlib
from datetime import datetime, timedelta
import time
from email.utils import parsedate_tz
import re
import string
from collections import defaultdict
import traceback


# Parse Twitter created_at datestring and turn it into
def to_datetime(datestring):
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6])
    return dt

def ck_coded_url(urlstring):
    cur.execute("""select code, hashtag from tweets_sample_test where url = %s and hashtag in ('ows','occupyoakland','occupyseattle') and date(created_at) between '2011-10-19' and '2012-04-30' and spike is null""", urlstring.encode("utf-8"))
    result = cur.fetchone()
    if result:
        return result
    else:
        return None

tweets = []
for line in open('Nikita_tweets.txt', 'r'):
    tweets.append(json.loads(line))
for tweet in tweets:
    tweet_id = tweet['id']
   
    tweet['stack_vars'] = { 'text_hash': None,
                            'created_ts': None,
                            'hashtags': [],
                            'mentions': [],
                            'codes': [],
                            'track_kw': {},
                            'entities_counts': {},
                            'user': {},    
                            'full_tweet': {}}
    
    tweet['stack_vars']["track_kw"] = { "org_tweet" : {}, 
                                        "rt_tweet" : {}, 
                                        "qt_tweet" : {}}
    expand_url=False
    
    track_list=[]
    if track_list:
        track_set = set(track_list)
    else:
        track_set = None

    
    #using get() to check if there is a value for the key 'retweeted_status' or 'extended_tweet'
    retweeted_status=tweet.get('retweeted_status')
    if(retweeted_status):
        long_retweet=retweeted_status.get('extended_tweet')
        if(long_retweet):
            print(tweet_id,"Long Retweet")
            tweet['stack_vars']['full_tweet'] = tweet['retweeted_status']['extended_tweet']     
            hashtag_num = 0
            tweet['stack_vars']['hashtags'] = []
            tweet['stack_vars']['mentions'] = []
            tweet['stack_vars']['codes'] = []
            
            #print("Full tweet:",tweet['stack_vars']['full_tweet'])
            #hashtags
            if "hashtags" in tweet['retweeted_status']['extended_tweet']['entities']:
                hashtag_num = len(tweet['retweeted_status']['extended_tweet']['entities']['hashtags'])
                for index in range(len(tweet['retweeted_status']['extended_tweet']['entities']['hashtags'])):
                    tweet['stack_vars']['hashtags'].append(tweet['retweeted_status']['extended_tweet']['entities']['hashtags'][index]['text'].lower())
            
            #mentions
            mentions_num = 0
            if "user_mentions" in tweet['retweeted_status']['extended_tweet']['entities']:
                mentions_num = len(tweet['retweeted_status']['extended_tweet']['entities']['user_mentions'])
                for index in range(len(tweet['retweeted_status']['extended_tweet']['entities']['user_mentions'])):
                    if "screen_name" in tweet['retweeted_status']['extended_tweet']['entities']['user_mentions'][index]:
                        tweet['stack_vars']['mentions'].append(tweet['retweeted_status']['extended_tweet']['entities']['user_mentions'][index]['screen_name'].lower())
             
            
            #codes
            urls_num = 0
            coded_url_num = 0
            urls = []
        
            if "urls" in tweet['retweeted_status']['extended_tweet']['entities']:
                urls_num = len(tweet['retweeted_status']['extended_tweet']['entities']['urls'])
            
                if expand_url:
                    for urls in tweet['retweeted_status']['extended_tweet']['entities']['urls']:
                        url_code = None
                        if 'long-url' in urls and urls['long-url'] is not None:
                            url_code = ck_coded_url(urls['long-url'])
                        elif "expanded_url" in urls and urls['expanded_url'] is not None:
                            url_code = ck_coded_url(urls['expanded_url'])
                        elif "url" in urls:
                            url_code = ck_coded_url(urls['url'])
                    
                        if url_code:
                            urls['code'] = url_code[0]
                            urls['hashtag'] = url_code[1]
                            tweet['stack_vars']['codes'].append(url_code[0])
                        
                coded_url_num = len(tweet['stack_vars']['codes'])
            
            #entities counts
            tweet['stack_vars']['entities_counts'] = {  'urls': urls_num,
                                                        'hashtags': hashtag_num,
                                                        'user_mentions': mentions_num,
                                                        'coded_urls': coded_url_num }

            tweet['stack_vars']['hashtags'].sort()
            tweet['stack_vars']['mentions'].sort()
            
            #text hash
            tweet['stack_vars']['text_hash'] = hashlib.md5(tweet['retweeted_status']['extended_tweet']['full_text'].encode("utf-8")).hexdigest()
                
            if track_set:
                myURLs = []
                for index in range(len(tweet['retweeted_status']['extended_tweet']['entities']['urls'])):
                    myURLs.append(tweet['retweeted_status']['extended_tweet']['entities']['urls'][index]['expanded_url'].lower())
                
                hashTags_set = set([x.lower() for x in tweet['stack_vars']['hashtags']])
                mentions_set = set([x.lower() for x in tweet['stack_vars']['mentions']])
            
                track_set = set([x.lower() for x in track_set])
                tweet['stack_vars']["track_kw"]["org_tweet"]["hashtags"] = list(set(hashTags_set).intersection(track_set))
                tweet['stack_vars']["track_kw"]["org_tweet"]["mentions"] = list(set(mentions_set).intersection(track_set))
            
                tweet_text = re.sub('[%s]' % punct, ' ', tweet['text'])
                tweet_text = emoji_pattern.sub(r'', tweet_text)
                tweet_text = tweet_text.lower().split()
            
                tweet['stack_vars']["track_kw"]["org_tweet"]["text"] = list(set(tweet_text).intersection(track_set))
            
                tmpURLs = []
                for url in myURLs:
                    for x in track_set:
                        if x in url:
                            tmpURLs.append(url)
                tweet['stack_vars']["track_kw"]["org_tweet"]["urls"] = list(tmpURLs)
                
            t = to_datetime(tweet['created_at'])
            tweet['stack_vars']['created_ts'] = t.strftime('%Y-%m-%d %H:%M:%S')
    
            t = to_datetime(tweet['user']['created_at'])
            tweet['stack_vars']['user']['created_ts'] = t.strftime('%Y-%m-%d %H:%M:%S')
            
            print("Hashtags:",tweet['stack_vars']['hashtags'])
            print("Mentions:",tweet['stack_vars']['mentions'])
            print("Codes:",tweet['stack_vars']['codes'])
            print("Full Tweet:",tweet['stack_vars']['full_tweet'])
            print("Tweet created at:",tweet['stack_vars']['created_ts'])
            print("text hash:",tweet['stack_vars']['text_hash'])
            print("Entitities Count:",tweet['stack_vars']['entities_counts'])
            print("Track_kw",tweet['stack_vars']["track_kw"])
            print("user:",tweet['stack_vars']['user'])
        elif not long_retweet:
            print(tweet_id,"Short Retweet")
            tweet['stack_vars']['full_tweet'] = tweet['retweeted_status']    
            hashtag_num = 0
            tweet['stack_vars']['hashtags'] = []
            tweet['stack_vars']['mentions'] = []
            tweet['stack_vars']['codes'] = []
            #print("Full tweet:",tweet['stack_vars']['full_tweet'])
            
            #hashtags
            if "hashtags" in tweet['retweeted_status']['entities']:
                hashtag_num = len(tweet['retweeted_status']['entities']['hashtags'])
                for index in range(len(tweet['retweeted_status']['entities']['hashtags'])):
                    tweet['stack_vars']['hashtags'].append(tweet['retweeted_status']['entities']['hashtags'][index]['text'].lower())
            
            #mentions
            mentions_num = 0
            if "user_mentions" in tweet['retweeted_status']['entities']:
                mentions_num = len(tweet['retweeted_status']['entities']['user_mentions'])
                for index in range(len(tweet['retweeted_status']['entities']['user_mentions'])):
                    if "screen_name" in tweet['retweeted_status']['entities']['user_mentions'][index]:
                        tweet['stack_vars']['mentions'].append(tweet['retweeted_status']['entities']['user_mentions'][index]['screen_name'].lower())
            
            #codes
            urls_num = 0
            coded_url_num = 0
            urls = []
        
            if "urls" in tweet['retweeted_status']['entities']:
                urls_num = len(tweet['retweeted_status']['entities']['urls'])
            
                if expand_url:
                    for urls in tweet['retweeted_status']['entities']['urls']:
                        url_code = None
                        if 'long-url' in urls and urls['long-url'] is not None:
                            url_code = ck_coded_url(urls['long-url'])
                        elif "expanded_url" in urls and urls['expanded_url'] is not None:
                            url_code = ck_coded_url(urls['expanded_url'])
                        elif "url" in urls:
                            url_code = ck_coded_url(urls['url'])
                    
                        if url_code:
                            urls['code'] = url_code[0]
                            urls['hashtag'] = url_code[1]
                            tweet['stack_vars']['codes'].append(url_code[0])
                        
                coded_url_num = len(tweet['stack_vars']['codes'])
            
            #entities counts
            tweet['stack_vars']['entities_counts'] = {  'urls': urls_num,
                                                        'hashtags': hashtag_num,
                                                        'user_mentions': mentions_num,
                                                        'coded_urls': coded_url_num }

            tweet['stack_vars']['hashtags'].sort()
            tweet['stack_vars']['mentions'].sort()
            
            #text hash
            #tweet['stack_vars']['text_hash'] = hashlib.md5(tweet['retweeted_status']['full_text'].encode("utf-8")).hexdigest()
            if track_set:
                myURLs = []
                for index in range(len(tweet['retweeted_status']['entities']['urls'])):
                    myURLs.append(tweet['retweeted_status']['entities']['urls'][index]['expanded_url'].lower())
                
                hashTags_set = set([x.lower() for x in tweet['stack_vars']['hashtags']])
                mentions_set = set([x.lower() for x in tweet['stack_vars']['mentions']])
            
                track_set = set([x.lower() for x in track_set])
                tweet['stack_vars']["track_kw"]["org_tweet"]["hashtags"] = list(set(hashTags_set).intersection(track_set))
                tweet['stack_vars']["track_kw"]["org_tweet"]["mentions"] = list(set(mentions_set).intersection(track_set))
            
                tweet_text = re.sub('[%s]' % punct, ' ', tweet['text'])
                tweet_text = emoji_pattern.sub(r'', tweet_text)
                tweet_text = tweet_text.lower().split()
            
                tweet['stack_vars']["track_kw"]["org_tweet"]["text"] = list(set(tweet_text).intersection(track_set))
            
                tmpURLs = []
                for url in myURLs:
                    for x in track_set:
                        if x in url:
                            tmpURLs.append(url)
                tweet['stack_vars']["track_kw"]["org_tweet"]["urls"] = list(tmpURLs)
                
            t = to_datetime(tweet['created_at'])
            tweet['stack_vars']['created_ts'] = t.strftime('%Y-%m-%d %H:%M:%S')
    
            t = to_datetime(tweet['user']['created_at'])
            tweet['stack_vars']['user']['created_ts'] = t.strftime('%Y-%m-%d %H:%M:%S')
                       
            print("Hashtags:",tweet['stack_vars']['hashtags'])
            print("Mentions:",tweet['stack_vars']['mentions'])
            print("Codes:",tweet['stack_vars']['codes'])
            print("Full Tweet:",tweet['stack_vars']['full_tweet'])
            print("Tweet created at:",tweet['stack_vars']['created_ts'])
            print("text hash:",tweet['stack_vars']['text_hash'])
            print("Entitities Count:",tweet['stack_vars']['entities_counts'])
            print("Track_kw",tweet['stack_vars']["track_kw"])
            print("user:",tweet['stack_vars']['user'])
    elif(not retweeted_status):
        long_tweet=tweet.get('extended_tweet')
        if long_tweet:
            print(tweet_id,"Long Tweet")
            tweet['stack_vars']['full_tweet'] = tweet['extended_tweet']     
            hashtag_num = 0
            tweet['stack_vars']['hashtags'] = []
            tweet['stack_vars']['mentions'] = []
            tweet['stack_vars']['codes'] = []
            
            #print("Full tweet:",tweet['stack_vars']['full_tweet'])
            
            #hashtags
            if "hashtags" in tweet['extended_tweet']['entities']:
                hashtag_num = len(tweet['extended_tweet']['entities']['hashtags'])
                for index in range(len(tweet['extended_tweet']['entities']['hashtags'])):
                    tweet['stack_vars']['hashtags'].append(tweet['extended_tweet']['entities']['hashtags'][index]['text'].lower())
            
            #mentions
            if "user_mentions" in tweet['extended_tweet']['entities']:
                mentions_num = len(tweet['extended_tweet']['entities']['user_mentions'])
                for index in range(len(tweet['extended_tweet']['entities']['user_mentions'])):
                    if "screen_name" in tweet['extended_tweet']['entities']['user_mentions'][index]:
                        tweet['stack_vars']['mentions'].append(tweet['extended_tweet']['entities']['user_mentions'][index]['screen_name'].lower())
            
            #codes
            urls_num = 0
            coded_url_num = 0
            urls = []
        
            if "urls" in tweet['extended_tweet']['entities']:
                urls_num = len(tweet['extended_tweet']['entities']['urls'])
            
                if expand_url:
                    for urls in tweet['extended_tweet']['entities']['urls']:
                        url_code = None
                        if 'long-url' in urls and urls['long-url'] is not None:
                            url_code = ck_coded_url(urls['long-url'])
                        elif "expanded_url" in urls and urls['expanded_url'] is not None:
                            url_code = ck_coded_url(urls['expanded_url'])
                        elif "url" in urls:
                            url_code = ck_coded_url(urls['url'])
                    
                        if url_code:
                            urls['code'] = url_code[0]
                            urls['hashtag'] = url_code[1]
                            tweet['stack_vars']['codes'].append(url_code[0])
                        
                coded_url_num = len(tweet['stack_vars']['codes'])
            
            #entities counts
            tweet['stack_vars']['entities_counts'] = {  'urls': urls_num,
                                                        'hashtags': hashtag_num,
                                                        'user_mentions': mentions_num,
                                                        'coded_urls': coded_url_num }

            tweet['stack_vars']['hashtags'].sort()
            tweet['stack_vars']['mentions'].sort()
            
            #text hash
            tweet['stack_vars']['text_hash'] = hashlib.md5(tweet['extended_tweet']['full_text'].encode("utf-8")).hexdigest()
            if track_set:
                myURLs = []
                for index in range(len(tweet['extended_tweet']['entities']['urls'])):
                    myURLs.append(tweet['extended_tweet']['entities']['urls'][index]['expanded_url'].lower())
                
                hashTags_set = set([x.lower() for x in tweet['stack_vars']['hashtags']])
                mentions_set = set([x.lower() for x in tweet['stack_vars']['mentions']])
            
                track_set = set([x.lower() for x in track_set])
                tweet['stack_vars']["track_kw"]["org_tweet"]["hashtags"] = list(set(hashTags_set).intersection(track_set))
                tweet['stack_vars']["track_kw"]["org_tweet"]["mentions"] = list(set(mentions_set).intersection(track_set))
            
                tweet_text = re.sub('[%s]' % punct, ' ', tweet['text'])
                tweet_text = emoji_pattern.sub(r'', tweet_text)
                tweet_text = tweet_text.lower().split()
            
                tweet['stack_vars']["track_kw"]["org_tweet"]["text"] = list(set(tweet_text).intersection(track_set))
            
                tmpURLs = []
                for url in myURLs:
                    for x in track_set:
                        if x in url:
                            tmpURLs.append(url)
                tweet['stack_vars']["track_kw"]["org_tweet"]["urls"] = list(tmpURLs)
                
            t = to_datetime(tweet['created_at'])
            tweet['stack_vars']['created_ts'] = t.strftime('%Y-%m-%d %H:%M:%S')
    
            t = to_datetime(tweet['user']['created_at'])
            tweet['stack_vars']['user']['created_ts'] = t.strftime('%Y-%m-%d %H:%M:%S')
            
            print("Hashtags:",tweet['stack_vars']['hashtags'])
            print("Mentions:",tweet['stack_vars']['mentions'])
            print("Codes:",tweet['stack_vars']['codes'])
            print("Full Tweet:",tweet['stack_vars']['full_tweet'])
            print("Tweet created at:",tweet['stack_vars']['created_ts'])
            print("text hash:",tweet['stack_vars']['text_hash'])
            print("Entitities Count:",tweet['stack_vars']['entities_counts'])
            print("Track_kw",tweet['stack_vars']["track_kw"])
            print("user:",tweet['stack_vars']['user'])
    elif not long_tweet:
        print(tweet_id,"Short Tweet")
        tweet['stack_vars']['full_tweet'] = tweet['text']     
        hashtag_num = 0
        tweet['stack_vars']['hashtags'] = []
        tweet['stack_vars']['mentions'] = []
        tweet['stack_vars']['codes'] = []
        #print("Full tweet:",tweet['stack_vars']['full_tweet'])
        #hashtags
        if "hashtags" in tweet['entities']:
            hashtag_num = len(tweet['entities']['hashtags'])
            for index in range(len(tweet['entities']['hashtags'])):
                tweet['stack_vars']['hashtags'].append(tweet['entities']['hashtags'][index]['text'].lower())  
        #mentions
        if "user_mentions" in tweet['entities']:
            mentions_num = len(tweet['entities']['user_mentions'])
            for index in range(len(tweet['entities']['user_mentions'])):
                if "screen_name" in tweet['entities']['user_mentions'][index]:
                    tweet['stack_vars']['mentions'].append(tweet['entities']['user_mentions'][index]['screen_name'].lower())
        
        #codes
        urls_num = 0
        coded_url_num = 0
        urls = []
        
        if "urls" in tweet['extended_tweet']['entities']:
            urls_num = len(tweet['extended_tweet']['entities']['urls'])
            
            if expand_url:
                for urls in tweet['extended_tweet']['entities']['urls']:
                    url_code = None
                    if 'long-url' in urls and urls['long-url'] is not None:
                        url_code = ck_coded_url(urls['long-url'])
                    elif "expanded_url" in urls and urls['expanded_url'] is not None:
                        url_code = ck_coded_url(urls['expanded_url'])
                    elif "url" in urls:
                        url_code = ck_coded_url(urls['url'])
                    
                    if url_code:
                        urls['code'] = url_code[0]
                        urls['hashtag'] = url_code[1]
                        tweet['stack_vars']['codes'].append(url_code[0])
                        
            coded_url_num = len(tweet['stack_vars']['codes'])
            
        #entities counts
        tweet['stack_vars']['entities_counts'] = {  'urls': urls_num,
                                                    'hashtags': hashtag_num,
                                                    'user_mentions': mentions_num,
                                                    'coded_urls': coded_url_num }

        tweet['stack_vars']['hashtags'].sort()
        tweet['stack_vars']['mentions'].sort()
            
        #text hash
        tweet['stack_vars']['text_hash'] = hashlib.md5(tweet['full_text'].encode("utf-8")).hexdigest()
        
        if track_set:       
            myURLs = []
            for index in range(len(tweet['entities']['urls'])):
                myURLs.append(tweet['entities']['urls'][index]['expanded_url'].lower())
                
            hashTags_set = set([x.lower() for x in tweet['stack_vars']['hashtags']])
            mentions_set = set([x.lower() for x in tweet['stack_vars']['mentions']])
            
            track_set = set([x.lower() for x in track_set])
            tweet['stack_vars']["track_kw"]["org_tweet"]["hashtags"] = list(set(hashTags_set).intersection(track_set))
            tweet['stack_vars']["track_kw"]["org_tweet"]["mentions"] = list(set(mentions_set).intersection(track_set))
            
            tweet_text = re.sub('[%s]' % punct, ' ', tweet['text'])
            tweet_text = emoji_pattern.sub(r'', tweet_text)
            tweet_text = tweet_text.lower().split()
            
            tweet['stack_vars']["track_kw"]["org_tweet"]["text"] = list(set(tweet_text).intersection(track_set))
            
            tmpURLs = []
            for url in myURLs:
                for x in track_set:
                    if x in url:
                        tmpURLs.append(url)
            tweet['stack_vars']["track_kw"]["org_tweet"]["urls"] = list(tmpURLs)
            
        t = to_datetime(tweet['created_at'])
        tweet['stack_vars']['created_ts'] = t.strftime('%Y-%m-%d %H:%M:%S')
    
        t = to_datetime(tweet['user']['created_at'])
        tweet['stack_vars']['user']['created_ts'] = t.strftime('%Y-%m-%d %H:%M:%S')
            
        print("Hashtags:",tweet['stack_vars']['hashtags'])
        print("Mentions:",tweet['stack_vars']['mentions'])
        print("Codes:",tweet['stack_vars']['codes'])
        print("Full Tweet:",tweet['stack_vars']['full_tweet'])
        print("Tweet created at:",tweet['stack_vars']['created_ts'])
        print("text hash:",tweet['stack_vars']['text_hash'])
        print("Entitities Count:",tweet['stack_vars']['entities_counts'])  
        print("Track_kw",tweet['stack_vars']["track_kw"])
        print("user:",tweet['stack_vars']['user'])           
            

            
