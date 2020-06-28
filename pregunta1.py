import twitter
import time
import os
from dotenv import load_dotenv


def main():
    while True:
        ConsultarTweets()
        time.sleep(20)


def ConsultarTweets():
    load_dotenv()
    num = 0
    api = twitter.Api(consumer_key=os.environ.get("CONSUMER_KEY"),
                      consumer_secret=os.environ.get("CONSUMER_SECRET"),
                      access_token_key=os.environ.get("ACCESS_TOKEN_KEY"),
                      access_token_secret=os.environ.get("ACCESS_TOKKEN_SECRET"))

    results = api.GetSearch(
        raw_query="q=covid%20&result_type=recent&since=2014-07-19&count=10")

    for i in results:
        print(str(num + 1) + ": " + results[num].text)
        num = num + 1


if __name__ == "__main__":
    main()
