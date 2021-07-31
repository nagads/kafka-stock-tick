#https://pypi.org/project/websocket_client/
import websocket
import uuid
import datetime
import json
from kafka import KafkaProducer

folderName = "certs/"
producer = KafkaProducer(
    bootstrap_servers="kafka-stock-tick-sample-koride-7a85.aivencloud.com:20158",
    #bootstrap_servers="<INSTANCE_NAME>-<PROJECT_NAME>.aivencloud.com:<PORT>",
    security_protocol="SSL",
    ssl_cafile=folderName+"ca.pem",
    ssl_certfile=folderName+"service.cert",
    ssl_keyfile=folderName+"service.key",
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer=lambda v: json.dumps(v).encode('ascii')

)

def on_message(ws, message):
    uuidgen = str(uuid.uuid4())
    timestamp = datetime.datetime.utcnow().isoformat()
    result = json.loads(message)
    result["timestamp"] = timestamp
    #result["uuid"] = uuidgen
    #message = "{\"timestamp\":"+timestamp+message+"}"
    print("key: "+uuidgen)
    #print("value: ")
    print(json.dumps(result))
    producer.send("stock-ticks",
                  key={"key": uuidgen},
                  value={"message": json.dumps(result)}
                  )
    producer.flush()

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
   #ws.send('{"type":"subscribe","symbol":"AAPL"}')
    #s.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
   #ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=c41r8h2ad3iegm5g7k1g",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()