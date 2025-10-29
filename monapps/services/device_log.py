from typing import Literal

from django.db.models import Model

from utils.ts_utils import create_dt_from_ts_ms
from utils.db_field_utils import get_instance_full_id


# reflects the alarms that come from different devices (or datastreams)
# these alarms can be tied to timestamps in the past, therefore a timestamp is mandatory
# at the moment - simply put messages into the console
# but later it's necessary to add these records to the db
def add_to_device_log(
    type: Literal["ERROR", "WARNING", "INFO"], msg: str, ts: int, instance: Model | str = "Unknown", status: str = ""
):

    dt_str = create_dt_from_ts_ms(ts).isoformat(timespec="milliseconds")

    if not status:
        status = "IN"

    if isinstance(instance, Model):
        instance_id = get_instance_full_id(instance)
    else:
        instance_id = instance
    print(f"[DEVICE LOG]\t[{type}]\t[{status.upper()}]\t{dt_str}\t{instance_id}\t{msg}")
