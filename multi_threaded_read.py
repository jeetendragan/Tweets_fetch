import StdOutLinstener
import json
import threading
import os
import datetime
from tweepy import OAuthHandler
from tweepy import Stream

DATA_DIR = "/data/jeetendragan/"

def fetch_tweets(credentials, query):
    listener = StdOutLinstener.StdOutLinstener(DATA_DIR, credentials["id"], query)
    auth = OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    auth.set_access_token(credentials['ACCESS_TOKEN'], credentials['ACCESS_TOKEN_SECRET'])
    stream = Stream(auth, listener)
    stream.filter(track = query['track'])

if __name__ == '__main__':
    try:
        cred_file = open("credentials.json")
        cred_json_contents = cred_file.read()
        cred_file.close()
        all_credentials = json.loads(cred_json_contents)
    except IOError:
        print("\nERROR: Please make sure that credentials.json file in present.\n")
    except ValueError:
        print("\nERROR: The credentials.json file should have an array of credential objects. Make sure that the format is correct\n")
    
    try:
        query_file = open("query.json")
        query_json_contents = query_file.read()
        query_file.close()

        all_queries = json.loads(query_json_contents)
    except IOError:
        print("\nERROR: Please make sure that query.json file in present.\n")
    except ValueError:
        print("\nERROR: The query.json file should have an array of query objects. Make sure that the format is correct\n")

    threads = {}
    for credentials in all_credentials:
        thread = threading.Thread(target = fetch_tweets, args = (credentials, all_queries[credentials['id']]))
        thread.start()
        threads[credentials["id"]] = thread