import numpy as np
import pandas as pd
import json
from simplejson import JSONDecodeError
from datetime import datetime
import matplotlib.pyplot as plt
import os
import gzip
import random

DATA_DIR = "/data/kenny/dnc2020/"
WRITE_DIR = "/data/jeetendragan/dnc2020/"
REPORT_DIR = "/data/jeetendragan/dnc2020/reports/"
CHUNK_DIR = "/data/jeetendragan/dnc2020/chunks/"

dir_paths = [os.path.join(DATA_DIR,w) for w in os.listdir(DATA_DIR) if '.txt' not in w and '.json' not in w]

output_zip_paths = [os.path.join(parent_dir,zip_file) for parent_dir in dir_paths for zip_file in os.listdir(parent_dir)]

print("Starting data aggregation")

candidate_ref = {'Elizabeth Warren': set(['Elizabeth Warren', 'Warren','@ewarren', '#imWithHer', '#TeamWarren', '#Warren', '#Warren2020', '#WarrenHarris', '#WarrenIsaSnake', '#WarrenCastro2020']),
 'Michael Bennet':set(['@MichaelBennet', 'Bennet']),
 'Joe Biden' : set(['@JoeBiden', 'Biden', '#Biden2020', '#JoeBiden', '#TeamJoe', '#TellTheTruthJoe', '#BidenIsCorrupt']),
 'Michael Bloomberg' : set(['@Mike2020', '#bloomberg2020', 'Bloomberg']),
 'Pete Buttigieg' : set(['@PeteButtigieg', 'Buttigieg', '#dropoutpete', '#pete2020', '#PeteForAmerica','#PeteforPresident', '#TeamPete', '#winecavepete']),
 'John Delaney' : set(['@JohnDelaney', 'Delaney']),
 'Tulsi Gabbard' : set(['Gabbard', 'Tulsi Gabbard', '@tulsigabbard', '#StandWithTulsi', '#tulsiwasright', '#tulsigabbard', '#Tulsi2020', '#Tulsi', '#tulsicoward']),
 'Amy Klobuchar' : set(['Amy Klobuchar', 'Klobuchar', '@amyklobuchar', '#AmyforAmerica', '#klobucharmy', '#DropOutAmy']),
 'Deval Patrick' : set(['Deval Patrick', '@DevalPatrick', '#Devalforall', '#DevalPatrick2020', '#devalpatrick']),
 'Bernie Sanders' : set(['Sanders', 'Bernie Sanders', '@BernieSanders', '#Bernie', '#berniesanders', '#bernie2020', '#Fightfor15', '#ILikeBernie', '#NobodyLikesHim', '#NotMeUs', '#TrustBernie', '#ITrustBernie', '#VoteforBernie', '#WomenForBernie', '#blackwomenforbernie']),
 'Tom Steyer' : set(['Steyer','Tom Steyer','@TomSteyer', '#momenTOM', '#TomforPresident']),
 'Andrew Yang': set(['Andrew Yang','Yang', '@AndrewYang', '#AmericaNeedsYang', '#AndrewYang', '#NHforYang', '#Yang2020', '#YangGang']),
}


def get_empty_candidate_stats():
    candidate_stats = {}
    for candidate in candidate_ref:
        candidate_stats[candidate] = { 'tweet_count': 0, 'pos': 0, 'neg': 0, 'neu': 0}
    return candidate_stats


sentiments = ['pos', 'neg', 'neu']
def get_tweet_sentiment(tweet):
    return sentiments[random.randrange(0, 3)]


def get_candidates_referred(tweet_text):
    refers_to = []
    for cand in candidate_ref:
        for ref in candidate_ref[cand]:
            if ref in tweet_text:
                refers_to.append(cand)
                break
    return refers_to


def get_stats_for_tweet(tweet_text):
    # detect the sentiment of the tweet
    sentiment = get_tweet_sentiment(tweet_text)
    
    references_to = get_candidates_referred(tweet_text)
    
    return {'sentiment': sentiment, 'references_to': references_to}



count = 0
current_day = ""
candidate_stats = get_empty_candidate_stats()
report = {}
for file_path in output_zip_paths:
    if count == 2:
        break
    try:
        # read all the contents of the file
        file_handler = gzip.open(file_path)
        contents = file_handler.read().decode("utf-8")
        file_handler.close()
        if contents == "":
            continue
        
        # mark a successful read
        count = count + 1
        
        #all_tweets.append(contents)
        # split the contents by '\n'. We now have an array of tweet strings
        print("Splitting tweets..")
        this_tweets = contents.split('\n')
        
        # convert all the tweets to an object
        print("Converting tweets to json...")
        this_tweets_json = [json.loads(tweet) for tweet in this_tweets if tweet != ""]

        print('Parsing tweets...')
        # iterate over all the tweet objects - Filter by date, find references to candidates, and find stats
        for tweet_json in this_tweets_json:
            filtered_tweet = {}
            
            # get the date of the tweet object
            tweet_date = datetime.strptime(tweet_json['created_at'], '%a %b %d %X %z %Y').strftime('%d/%m/%Y')
            
            # build a new tweet object with less variables
            filtered_tweet['created_at'] = tweet_json['created_at']
            filtered_tweet['id'] = tweet_json['id']
            filtered_tweet['text'] = tweet_json['text']

            # detect who this tweet is referred to
            # get tweet stats returns the list of candidates the tweet has been referred to 
            # and the sentiment of the tweet
            tweet_stats = get_stats_for_tweet(tweet_json['text'])

            # if a tweet has not been seen on the tweet_date, then create a new object
            if tweet_date not in report:
                report[tweet_date] = get_empty_candidate_stats()
            
            for cand in tweet_stats['references_to']:
                report[tweet_date][cand][tweet_stats['sentiment']] += 1
                report[tweet_date][cand]['tweet_count'] += 1
            
            print(tweet_stats['sentiment']+", "+','.join(tweet_stats['references_to']))
            
        print("Read contents of file: "+file_path)
    except Exception as e:
        print("Exception while reading file: "+file_path)
        print(format(e))


