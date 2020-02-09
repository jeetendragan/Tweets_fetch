import StdOutListener
import json
import threading
import os
import datetime
from tweepy import OAuthHandler
from tweepy import Stream
import time
import sys
import time

def fetch_tweets(credentials, query, data_dir):
    auth = OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    auth.set_access_token(credentials['ACCESS_TOKEN'], credentials['ACCESS_TOKEN_SECRET'])

    while True:
        now = str(datetime.datetime.now())
        path = os.path.join(data_dir, now + "--" + credentials["id"])
        os.mkdir(path)
        listener = StdOutListener.StdOutListener(stream_id = credentials["id"],
                                                 path=path)
        stream = Stream(auth, listener)
        try:
            stream.filter(**query)
        except KeyboardInterrupt:
            # stop only when Ctr+C is pressed
            print("Stopping streaming now!")
            break
        except BaseException as e:
            print("******EXCPTION HANDLED********")
            print("Error: "+str(e))
            log_file = open(path+"LogFile.txt", "a")
            log_file.write(str(e))
            log_file.close()
            print("Restarting in 3 minutes")
            time.sleep(180)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: mult_threaded_read.py [creds_file] [query_file] [data_dir]")
        sys.exit(-1)

    cred_filename = sys.argv[1]
    query_filename = sys.argv[2]
    data_dir = sys.argv[3]
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    try:
        cred_file = open(cred_filename)
        cred_json_contents = cred_file.read()
        cred_file.close()
        all_credentials = json.loads(cred_json_contents)
    except IOError:
        print("\nERROR: Please make sure that credentials.json file in present.\n")
    except ValueError:
        print("\nERROR: The credentials.json file should have an array of credential objects. Make sure that the format is correct\n")

    try:
        query_file = open(query_filename)
        query_json_contents = query_file.read()
        query_file.close()

        all_queries = json.loads(query_json_contents)
    except IOError:
        print("\nERROR: Please make sure that query.json file in present.\n")
    except ValueError:
        print("\nERROR: The query.json file should have an array of query objects. Make sure that the format is correct\n")

    threads = {}
    for credentials in all_credentials:
        thread = threading.Thread(target = fetch_tweets, args = (credentials, all_queries[credentials['id']], data_dir))
        thread.daemon = True
        thread.start()
        threads[credentials["id"]] = thread

    while True:
        time.sleep(3)

    #for thread in threads:
    #    thread.join()