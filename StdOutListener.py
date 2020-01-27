from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json
import datetime
import os
import gzip

class StdOutListener(StreamListener):

    def __init__(self, stream_id, path):
        self.path = path
        self.stream_id = str(stream_id)
        # file to store all the correctly fetched tweets
        self.out_file = gzip.open(os.path.join(self.path,"output_"+stream_id+".txt.gz"),"w")

        # file to store failed tweets - i.e. those that do not have the text component
        self.out_file_fail = os.path.join(self.path,"fails_"+stream_id+".txt")

        # file to store streaming failures - errors
        self.out_file_error = os.path.join(self.path, "errors_"+stream_id+".txt")
    
        self.stream_counter = 0

    def on_data(self, data):
        data_json = json.loads(data)
        self.stream_counter +=1
        print("{i}: {c}".format(i=self.stream_id,c=self.stream_counter))
        try:
            if 'in_reply_to_status_id' in data_json:
                self.out_file.write((json.dumps(data_json)+"\n").encode())
            else:
                print("{i}: not status".format(i=self.stream_id))
                self.stream_counter -= 1
        except JSONDecodeError:
            print("Failed JSON Decode")
        #else:
            #print('Failed')
            #print(data_json)
            #f = open(self.out_file_fail, 'a')
            #f.write(json.dumps(data_json)+"\n")
            #f.close()
        return True        
    
    def on_error(self, status):
        now = str(datetime.datetime.now())
        f = open(self.out_file_error, "w")
        f.write(now + "\t" + str(status) + "\n")
        f.close()
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

    credentials = all_credentials[1] # get the first set of credentials
    listener = StdOutListener(stream_id="1",path="./")

    auth = OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    auth.set_access_token(credentials['ACCESS_TOKEN'], credentials['ACCESS_TOKEN_SECRET'])
    stream = Stream(auth, listener)
    stream.filter(**all_queries[credentials['id']])