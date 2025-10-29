import signal
import os
import json
import logging
import traceback

from django.core.management.base import BaseCommand

import paho.mqtt.client as mqtt

from services.raw_data_processor import RawDataProcessor

from services.alarm_log import add_to_alarm_log

logger = logging.getLogger("#mqtt_sub")


def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        add_to_alarm_log("INFO", "Connected to the broker", instance="MQTT Sub")
        logger.info("MQTT subscriber connected")
        sub_topic = os.getenv("MQTT_SUB_TOPIC", "rawdata/#")
        logger.info(f"MQTT subscriber is trying to subscribe to the topic: {sub_topic}")
        client.subscribe(sub_topic, qos=0)
    else:
        add_to_alarm_log(
            "ERROR",
            f"Failed to connect to the broker, reason code: {reason_code}",
            instance="MQTT Sub",
        )
        logger.error(f"MQTT subscriber failed to connect, reason code: {reason_code}")


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    add_to_alarm_log("INFO", "Subscribed", instance="MQTT Sub")
    logger.info("MQTT subscriber subscribed")


def on_message(client, userdata, msg):
    # a message topic should look like:
    # 1. "rawdata/<location>/<sublocation>/..." - then the payload can have data from several devices and look like
    # {
    #     "dev_ui1":
    #         {"1234567890123": {"e": {...}, "w": {...}, "i": {...}, "ds_name1": {...}, "ds_name2": {...}, ...}, ...}
    #     dev_ui2: {...},
    #     ...
    #    }

    msg_str = str(msg.payload.decode("utf-8"))
    if len(msg_str) > 20:
        msg_str_cropped = msg_str[0:20] + "..."
    else:
        msg_str_cropped = msg_str
    # add_to_alarm_log(
    #     "INFO",
    #     f"A message on the topic '{msg.topic}' received: '{msg_str_cropped}'",
    #     instance="MQTT Sub",
    # )
    logger.info(f"A message on the topic '{msg.topic}' received: '{msg_str_cropped}'")
    try:
        payload = json.loads(msg_str)
    except Exception:
        # add_to_alarm_log("ERROR", "Error while converting JSON", instance="MQTT Sub")
        logger.error(f"Error while converting JSON: {traceback.format_exc()}")
        return

    # special case - Chirpstack payload
    if "chirpstack" in msg.topic:
        if "deviceInfo" in payload and "object" in payload:
            RawDataProcessor(payload["deviceInfo"]["devEui"].lower(), payload["object"]).execute()
            return
        else:
            # add_to_alarm_log(
            #     "ERROR", "Incorrect Chirpstack payload", instance="MQTT Sub"
            # )
            logger.error("Incorrect Chirpstack payload")
            return

    # common case - usually payload from ESF
    for dev_ui, dev_payload in payload.items():
        if type(dev_payload) is not dict:
            # add_to_alarm_log(
            #     "WARNING", f"Incorrect payload for device '{dev_ui}'", instance="MQTT Sub"
            # )
            logger.warning(f"Incorrect payload for device '{dev_ui}'")
            continue
        dev_ui = dev_ui.lower()  # unify all 'dev_ui's stored in the db
        RawDataProcessor(dev_ui, dev_payload).execute()


def on_disconnect(client: mqtt.Client, userdata, flags, reason_code, properties):
    add_to_alarm_log("INFO", "Disconnected from the broker", instance="MQTT Sub")
    logger.info("MQTT subscriber disconnected")


class Command(BaseCommand):

    mqtt_subscriber = None

    def handle(self, *args, **kwargs):
        self.inner_run(**kwargs)

    def inner_run(self, **kwarg):
        Command.mqtt_subscriber = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id="monappsV3", clean_session=True
        )
        Command.mqtt_subscriber.on_connect = on_connect
        Command.mqtt_subscriber.on_subscribe = on_subscribe
        Command.mqtt_subscriber.on_message = on_message
        Command.mqtt_subscriber.on_disconnect = on_disconnect
        try:
            mqtt_broker_host = os.getenv("MQTT_BROKER_HOST")
            if not mqtt_broker_host:
                s = "MQTT_BROKER_HOST env variable is not set"
                add_to_alarm_log("ERROR", s, instance="MQTT Sub")
                logger.error(s)
                return
            Command.mqtt_subscriber.connect(mqtt_broker_host, 1883, 60)
        except Exception as e:
            add_to_alarm_log("ERROR", "Failed to connect", instance="MQTT Sub")
            logger.error(f"MQTT subscriber failed to connect, reason: {e}")
            Command.mqtt_subscriber = None
        else:
            add_to_alarm_log("INFO", "Created", instance="MQTT Sub")
            logger.info("MQTT subscriber created")
            Command.mqtt_subscriber.loop_forever()


def handler(signum, frame):
    if Command.mqtt_subscriber is not None:
        Command.mqtt_subscriber.disconnect()


signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)
