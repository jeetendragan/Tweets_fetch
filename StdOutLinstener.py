from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json
import datetime
import os

class StdOutLinstener(StreamListener):

    def __init__(self, path, query):
        """timestamp_file = open(self.path + "start_time.txt", "w")
        timestamp_file.write(now)
        timestamp_file.close()"""
        self.path = path
        
        # file to store all the correctly fetched tweets
        self.out_file = self.path + "output_"+','.join(query["track"])+".txt"

        # file to store failed tweets - i.e. those that do not have the text component
        self.out_file_fail = self.path + "fails.txt"

        # file to store streaming failures - errors
        self.out_file_error = self.path + "errors.txt"
    
    def on_data(self, data):
        data_json = json.loads(data)
        print("___________________")
        if 'text' in data_json:
            print(data_json['text'])

            f = open(self.out_file, "a")
            f.write(json.dumps(data_json)+"\n")
            f.close()
        else:
            print('Failed')
            f = open(self.out_file_fail, 'a')
            f.write(json.dumps(data_json)+"\n")
            f.close()
        return True        
    
    def on_error(self, status):
        f = open(self.out_file_error, "w")
        f.write(str(status))
        f.close
        return True

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

    credentials = all_credentials[0] # get the first set of credentials
    listener = StdOutLinstener("/data/jeetendragan/", all_queries["1"])

    auth = OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    auth.set_access_token(credentials['ACCESS_TOKEN'], credentials['ACCESS_TOKEN_SECRET'])
    stream = Stream(auth, listener)
    stream.filter(track = ['Trump'])