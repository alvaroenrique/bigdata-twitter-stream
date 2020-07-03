from pyspark import SparkContext
from pyspark.sql import SQLContext
import twitter
import redis
import os
from dotenv import load_dotenv


def getTweetsWords(q, until_date, count=10):
    load_dotenv()
    api = twitter.Api(consumer_key=os.environ.get("CONSUMER_KEY"),
                      consumer_secret=os.environ.get("CONSUMER_SECRET"),
                      access_token_key=os.environ.get("ACCESS_TOKEN_KEY"),
                      access_token_secret=os.environ.get("ACCESS_TOKKEN_SECRET"))

    tweets = api.GetSearch(
        raw_query=f'q={q}%20&result_type=recent&until={until_date}&lang=es&count={count}')

    words = []
    for tweet in tweets:
        # [(palabra, 1), (otra_palabra, 1)]
        words = words + list(map(lambda x: (x, 1), tweet.text.split(" ")))
    return words


def setDicInRedis(dic):
    redisClient = redis.StrictRedis(host='localhost',
                                    port=6379,
                                    db=1)
    redisClient.zadd('p3', dic)

    # get all words: zrange p3 0 -1
    # get all words with values: zrange p3 0 -1 withscores
    # get value of specific word: zscore p3 covid


def main():
    sc = SparkContext("local", "Pregunta 3")
    sqlContext = SQLContext(sc)
    rdd = sqlContext.createDataFrame(
        getTweetsWords("covid", "2020-06-30", "50"), ['text', 'count']
    ).rdd

    reduced = rdd.reduceByKey(
        lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False)

    print(dict(reduced.collect()))
    setDicInRedis(dict(reduced.collect()))


if __name__ == "__main__":
    main()
