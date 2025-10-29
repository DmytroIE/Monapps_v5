import os

# MQTT publisher settings
# this name will be included into the topic of published messages
MONAPP_INSTANCE_ID = os.environ.get("MONAPP_INSTANCE_ID", "some_instance")
