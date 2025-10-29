import os
import logging
import paho.mqtt.client as mqtt

from services.alarm_log import add_to_alarm_log

logger = logging.getLogger("#mqtt_pub")


def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None):
    client_id = client._client_id.decode("utf-8")
    if reason_code == 0:
        add_to_alarm_log(
            "INFO", "Connected to the broker", instance=client_id
        )
        logger.info(f"MQTT publisher {client_id} connected")
    else:
        add_to_alarm_log(
            "ERROR",
            f"Failed to connect to the broker, reason code: {reason_code}",
            instance=client_id,
        )
        logger.error(f"MQTT publisher {client_id} failed to connect, reason code: {reason_code}")


def on_disconnect(client: mqtt.Client, userdata, flags, reason_code, properties):
    client_id = client._client_id.decode("utf-8")
    add_to_alarm_log(
        "INFO", "Disconnected from the broker", instance=client_id
    )
    logger.info(f"MQTT publisher {client_id} disconnected")


# the publisher will be created only if MONAPP_PROC_NAME is set
proc_name = os.environ.get("MONAPP_PROC_NAME")
sub_topic = os.getenv("MQTT_SUB_TOPIC")  # in order not to create a publisher in the subscriber process
mqtt_publisher = None

if proc_name is None:
    add_to_alarm_log("INFO", "No PROC_NAME set, no MQTT publisher created", instance="MQTT Pub")
    logger.info("No PROC_NAME set, no MQTT publisher created")
elif sub_topic is not None:
    add_to_alarm_log("INFO", "It is a subscriber process, no MQTT publisher created", instance="MQTT Pub")
    logger.info("It is a subscriber process, no MQTT publisher created")
else:
    publisher_id = f"MQTT Pub {proc_name}"
    if len(publisher_id) > 22:
        publisher_id = publisher_id[:23]
    mqtt_publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=publisher_id, clean_session=True)
    mqtt_publisher.on_connect = on_connect
    mqtt_publisher.on_disconnect = on_disconnect
    try:
        mqtt_broker_host = os.getenv("MQTT_BROKER_HOST")
        if not mqtt_broker_host:
            add_to_alarm_log("ERROR", "MQTT_BROKER_HOST env variable is not set", instance=publisher_id)
            logger.error("MQTT_BROKER_HOST env variable is not set")
        else:
            mqtt_publisher.connect(mqtt_broker_host, 1883, 60)
    except Exception as e:
        add_to_alarm_log("ERROR", "MQTT subscriber failed to connect", instance=publisher_id)
        logger.error(f"MQTT publisher {publisher_id} failed to connect, reason: {e}")
        mqtt_publisher = None
    else:
        logger.info(f"MQTT publisher {publisher_id} created")
        add_to_alarm_log("INFO", "MQTT publisher created", instance=publisher_id)
        mqtt_publisher.loop_start()
