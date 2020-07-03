import socket
import time
import twitter
import redis
import os
from dotenv import load_dotenv


mi_socket = socket.socket()
mi_socket.bind(('localhost', 9000))
mi_socket.listen(5)

def getTweetsWords(q, until_date, count=1):
    load_dotenv()
    api = twitter.Api(consumer_key=os.environ.get("CONSUMER_KEY"),
                      consumer_secret=os.environ.get("CONSUMER_SECRET"),
                      access_token_key=os.environ.get("ACCESS_TOKEN_KEY"),
                      access_token_secret=os.environ.get("ACCESS_TOKKEN_SECRET"))

    tweets = api.GetSearch(
        raw_query=f'q={q}%20&result_type=recent&until={until_date}&lang=es&count={count}')

    return tweets[0].text # Esto es un tweet

while True:
    conexion, addr = mi_socket.accept()
    print("Conexión establecida")
    print(addr)
    current_data = getTweetsWords("covid", "2020-06-30", "1")
    print(current_data)
    conexion.send(bytes(current_data.encode(encoding='UTF-8')))

    conexion.close()
