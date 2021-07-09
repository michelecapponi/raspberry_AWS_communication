import sys
import ssl
import time
import logging, traceback
import paho.mqtt.client as mqtt
import json
from threading import Thread
import RPi.GPIO as GPIO
import Adafruit_DHT as dht22
import urllib.request


# qualche impostazione per i log di sistema
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(log_format)
logger.addHandler(handler)


# verifico la presenza di connessione
loop_value = 1
while loop_value == 1:
    try:
        urllib.request.urlopen("http://www.google.com").close()
        loop_value = 0
        logger.info("Connected to the internet network")
    except urllib.error.URLError:
        logger.info("Try to connect to the internet network")
        time.sleep(1)

pin_sensore_cucina = 18  # qui utilizzo numerazione BCM perchè la libreria Adafruit utilizza numerazione BCM
pin_sensore_soggiorno = 18
rele_cucina = 11
rele_soggiorno = 13
topic_cucina = "raspberry/cucina"
topic_soggiorno = "raspberry/soggiorno"
topic_data_out = "Dati_sensori"
delay = 60                          # variabile che contiene il tempo di attesa tra una pubblicazione e la successiva

GPIO.setmode(GPIO.BOARD)  # imposto la numerazione fisica dei pin di raspberry
GPIO.setup(rele_cucina, GPIO.OUT)  # imposto il pin fisico 11 come output, sarà utilizzato per il relè del condizionatore della cucina
GPIO.setup(rele_soggiorno, GPIO.OUT)  # imposto il pin fisico 13 come output, sarà utilizzato per il relè del condizionatore del soggiorno


class PublishingThread(Thread):                 #definisco una nuova classe PublishingThreading sottoclasse di thread
    def __init__(self, nomeThread=""):
        Thread.__init__(self)
        self.nome = nomeThread

    def run(self):
        while True:
            sendDataToBroker(pin_sensore_cucina,"cucina")
            sendDataToBroker(pin_sensore_soggiorno,"soggiorno")
            time.sleep(delay)

Publish_into_topic = PublishingThread()

# imposto i parametri di connessione all'endpoint

IoT_protocol_name = "x-amzn-mqtt-ca"
aws_iot_endpoint = "a1zpar684nxmr3-ats.iot.us-east-2.amazonaws.com"  # <random>.iot.<region>.amazonaws.com
url = "https://{}".format(aws_iot_endpoint)

# carico i certificati e la chiave privata per la connessione ssl
cert = "/home/pi/Desktop/progetti_python/progetto_elettrotecnica_industriale/chiavi_e_certificati/raspberry.cert.pem"
private = "/home/pi/Desktop/progetti_python/progetto_elettrotecnica_industriale/chiavi_e_certificati/raspberry.private.key"


def sendDataToBroker(pin_sensore, stanza=""):
    timestamp = int(time.time() * 1000)
    temperatura = round(lettura_Sensore(pin_sensore),1) #arrotondo ad una cifra dopo la virgola la lettura del sensore 
    data_out_dict = {"Stanza": stanza, "ID": timestamp, "temperatura": temperatura}
    data_out = json.dumps(data_out_dict)                #creo la stringa json da inviare al message broker
    logger.info("try to publish: {}".format(data_out))
    mqttc.publish(topic_data_out, data_out)


def lettura_Sensore(pin):
    h, t = dht22.read_retry(dht22.DHT22, pin)
    return t


def on_connect(mqttc, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    mqttc.subscribe([(topic_cucina, 0), (topic_soggiorno, 0)])


def on_message(mqttc, userdata,msg):  # riscrivo la callback on_message custom per la nostra applicazione. on_message verrà chiamata ogni volta che arriva un messaggio su uno dei topic sottoscritti
    if msg.topic == topic_cucina:
        setRele(msg,rele_cucina)
    elif msg.topic == topic_soggiorno:
        setRele(msg,rele_soggiorno)


def setRele(msg,rele):
    data = json.loads(msg.payload.decode('UTF-8'))
    if data["condizionatore"] == "acceso":
        GPIO.output(rele, True)  # imposto il pin di controllo del relè della cucina a 3.3 Volt
        logger.info("condizionatore {} acceso".format(msg.topic))
    elif data["condizionatore"] == "spento":
        GPIO.output(rele, False)  # imposto il pin  di controllo del relè della cucina a 0 Volt
        logger.info("conidizionatore {} spento".format(msg.topic))


def ssl_alpn():
    try:
        # debug print openssl version
        logger.info("open ssl version:{}".format(ssl.OPENSSL_VERSION))
        ssl_context = ssl.create_default_context()
        ssl_context.set_alpn_protocols([IoT_protocol_name])
        ssl_context.load_cert_chain(certfile=cert, keyfile=private)
        return ssl_context
    except Exception as e:
        print("exception ssl_alpn()")
        raise e


try:
    mqttc = mqtt.Client()
    ssl_context = ssl_alpn()
    mqttc.tls_set_context(context=ssl_context)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    logger.info("start connect")
    mqttc.connect(aws_iot_endpoint, port=443)
    logger.info("connect success")
    mqttc.loop_start()

except Exception as e:
    logger.error("exception ")
    logger.error("e obj:{}".format(vars(e)))
    logger.error("message:{}".format(e.message))
    traceback.print_exc(file=sys.stdout)

Publish_into_topic.start()  # start del thread di pubblicazione