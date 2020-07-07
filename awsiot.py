# Thomas Brijs 2020
# AWS MQTT Send/Receiver
#!/usr/bin/python3 -u
import paho.mqtt.client as mqttclient
import time
import datetime
import ssl
import json
import re
import string
import requests

mqttbroker = 'xxxxxxxxxxxxxx-ats.iot.eu-central-1.amazonaws.com'
mqttport = 8883

ca = "./certs/AmazonRootCA1.pem"
cert = "./certs/fddfabd0c3-certificate.pem.crt"
private = "./certs/fddfabd0c3-private.pem.key"

def on_connect(c, ud, flags, rc):
  if rc == 0:
    global ConnOk  # Use global variable
    ConnOk = True  # Signal connection
  else:
    print("* Connection failed")

def on_disconnect(client, userdata,rc=0):
   print("* Disconnected with status:  ", str(rc))

def on_message(c, ud, message):
    msg_value = message.payload.decode("utf-8")
    msg_value = msg_value.rstrip('\0')
    print(msg_value)


ConnOk = False
client = mqttclient.Client('REVPI_TEST', transport='tcp', clean_session=False)  # create new instance
#client.username_pw_set(mqttuser, password=mqttpass)
client.on_connect = on_connect  # attach function to callback
client.on_message = on_message  # attach function to callback
client.tls_set(ca_certs=ca,certfile=cert,keyfile=private)
#client.tls_insecure_set(False)
client.connect(mqttbroker, mqttport, 50)
client.loop_start()  # start the loop

while ConnOk != True:  # Wait for connection
  time.sleep(1)
client.subscribe([("#", 1)])
print("* Subscribed to all")
try:
  while True:
    time.sleep(10)
    message = {}
    timestamp = datetime.datetime.utcnow()
    message['Timestamp'] = timestamp.strftime('%Y-%m-%d-%H:%M:%S.%f')
    message['Value'] = 666
    client.publish('TEST/testmsg',json.dumps(message))
except KeyboardInterrupt:
  print("* Exiting")
  client.disconnect()
  client.loop_stop()
