from typing import Literal

from django.db.models import Model
from django.utils import timezone

from utils.ts_utils import create_dt_from_ts_ms
from utils.db_field_utils import get_instance_full_id


# reflects the alarms that happen in real time
# at the moment - simply put messages into the console
# but later it's necessary to add these records to the db
def add_to_alarm_log(
    type: Literal["ERROR", "WARNING", "INFO"],
    msg: str,
    ts: int | None = None,
    instance: Model | str = "Django",
    status: str = "",
):

    if not ts:
        dt_str = timezone.now().isoformat(timespec="milliseconds")
    else:
        dt_str = create_dt_from_ts_ms(ts).isoformat(timespec="milliseconds")

    if not status:
        status = "IN"

    if isinstance(instance, Model):
        instance_id = get_instance_full_id(instance)
    else:
        instance_id = instance
    print(f"[ALARM LOG]\t[{type}]\t[{status.upper()}]\t{dt_str}\t{instance_id}\t{msg}")
