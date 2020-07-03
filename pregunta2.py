from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
import twitter
import redis
import os
from dotenv import load_dotenv

redisClient = redis.StrictRedis(host='localhost',
                                    port=6379,
                                    db=0)

def procesar_palabras(palabras):
  for palabra in palabras.collect():
    current_value = redisClient.zscore('palabras', palabra[1])
    if (current_value == None):
      current_value = 0
    redisClient.zadd('palabras',{ palabra[0]: current_value + 1 })


def main():
  sc = SparkContext("local[2]", "Pregunta 2")
  scc = StreamingContext(sc, 3)

  lineas = scc.socketTextStream("localhost", 9000)
  palabras = lineas.flatMap(lambda linea: linea.split(" "))
  pares = palabras.map(lambda pal: (pal, 1))
  reducido = pares.reduceByKey(lambda x, y: x + y)

  reducido.foreachRDD(procesar_palabras)
  
  reducido.pprint()


  scc.start()
  scc.awaitTermination()

if __name__ == "__main__":
    main()