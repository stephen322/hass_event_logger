#!/usr/bin/env python3

# This program requires Home Assistant MQTT Event Stream integration https://www.home-assistant.io/integrations/mqtt_eventstream


import paho.mqtt.client as mqtt
from datetime import datetime
import subprocess
import signal
import sys, os
import json
import logging


# MODE new_state requires event message to contain "event_type" == "state_changed" and logs event_data/new_state if it exists, it excludes "context"
MODE = "new_state"  # new_state, everything, raw
TOPIC = "hass_events"
DIR = "/var/log/homeassistant/statedumps/"
FILENAMES = "%Y-%m-%d.ndjson"   # Daily files - passed through strftime()
ONCLOSE_CMD = ["zstd", "--rm", "--rsyncable", "-9"] # filename will be appended as last parameter

MQTT_SERVER="localhost"
MQTT_PORT=1883
MQTT_USERNAME=""
MQTT_PASSWORD=""

logging.getLogger().setLevel(logging.INFO)





signal.signal(signal.SIGCHLD, signal.SIG_IGN) #Ignore subprocesses so we don't have to clean up zombies

def file_output(line):
    now = datetime.now()

    selector = now.strftime(FILENAMES)
    if selector != file_output.last_sel:
        if file_output.file is not None and file_output.file.closed is False:
            file_output.file.close()
            if ONCLOSE_CMD:
                logging.info("Running command: " + " ".join(ONCLOSE_CMD + [file_output.filename]))
               	subprocess.Popen(ONCLOSE_CMD + [file_output.filename])

        file_output.filename = DIR + "/" + selector
        file_output.file = open(file_output.filename, 'a', buffering=1)

    file_output.file.write(line)
    file_output.file.write("\n")
    file_output.last_sel = selector

# file_output static vars
file_output.last_sel = None
file_output.file = None
file_output.filename = None


def on_connect1(client, userdata, flags, reason_code):
    # paho-mqtt on_connect version 1
    if reason_code == 4 or reason_code == 5:
        logging.fatal(f"Failed to connect to MQTT server with result code: Not Authorized")
        sys.exit(1)
    elif reason_code > 0:
        logging.error(f"Failed to connect to MQTT server with result code: {reason_code}, retrying...")
    else:
        logging.info(f"Connected to MQTT server")
        client.subscribe(TOPIC)

def on_connect2(client, userdata, flags, reason_code, properties):
    # paho-mqtt on_connect version 2
    if reason_code.getName() == "Not authorized":
        logging.fatal(f"Failed to connect to MQTT server with result code: {reason_code}")
        sys.exit(1)
    elif reason_code.is_failure:
        logging.error(f"Failed to connect to MQTT server with result code: {reason_code}, retrying...")
    else:
        logging.info(f"Connected to MQTT server")
        client.subscribe(TOPIC)


def on_message(client, userdata, msg):
    try:
        data = msg.payload.decode('utf-8')
        jdata = json.loads(data)
        if MODE=="new_state":
            if jdata.get("event_type") == "state_changed":
                try:
                    state_data = jdata["event_data"]["new_state"]
                    state_data.pop("context", None)
                except:
                    logging.error("Error parsing new_state")
                    # fallback to log whole event
                    file_output(json.dumps(jdata))
                    return
                else:
                    file_output(json.dumps(state_data))
                    return

        elif MODE=="everything":
            file_output(json.dumps(jdata))
        else:
            # MODE="raw"
            file_output(data)

    except json.JSONDecodeError:
        logging.error("Received message is not valid JSON")




# DIR sanity checks
if not DIR:
    DIR = os.getcwd()
if DIR[-1] == "/":
    DIR = DIR[:-1]
os.makedirs(DIR, exist_ok=True)

try:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect2
except AttributeError:
    # paho-mqtt version 1?
    client = mqtt.Client()
    client.on_connect = on_connect1

client.on_message = on_message

if MQTT_USERNAME != "" and MQTT_USERNAME is not None:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.connect(MQTT_SERVER, MQTT_PORT, 60)

client.loop_forever()
