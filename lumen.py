
import time
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import random

Broker = "192.168.2.112"

sub_topic = "sensor/instructions"    # receive messages on this topic

lumen =0
############### MQTT section ##################

# when connecting to mqtt do this;

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe(sub_topic)

# when receiving a mqtt message do this;

def on_message(client, userdata, msg):
    message = str(msg.payload)
    print(msg.topic+" "+message)
    display_sensehat(message)

def publish_mqtt(sensor_data):
    mqttc = mqtt.Client("python_pub")
    mqttc.connect(Broker, 1883)
    mqttc.publish(pub_topic, sensor_data)
    #mqttc.loop(2) //timeout = 2s

def on_publish(mosq, obj, mid):
    print("mid: " + str(mid))


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(Broker, 1883, 1)


while True:
    lumen = random.randint(0,10000)
    publish.single("numbers","lumen"+ str(lumen), hostname = Broker)
    time.sleep(1)
    print ("datos enviados"+ str(lumen))
