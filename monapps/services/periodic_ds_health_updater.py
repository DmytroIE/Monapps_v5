import logging
from django.db import transaction
from django.conf import settings

from apps.datastreams.models import Datastream
from common.constants import HealthGrades
from utils.ts_utils import create_now_ts_ms, create_iso_str_from_ts_ms
from utils.update_utils import enqueue_update, set_attr_if_cond

logger = logging.getLogger("#ds_health_upd")


class PeriodicDsHealthUpdater:
    def __init__(self):
        self.dev_map = {}

    @transaction.atomic
    def execute(self):
        now_ts = create_now_ts_ms()
        ds_qs = (
            Datastream.objects.filter(
                health_next_eval_ts__lte=now_ts,
            )
            .filter(is_enabled=True)
            .exclude(time_update__isnull=True)
            .order_by("health_next_eval_ts")
            .prefetch_related("parent")
            .select_for_update()[: settings.MAX_DS_TO_HEALTH_PROC]
        )

        if len(ds_qs) == 0:
            return

        for ds in ds_qs:
            self.update_ds(ds)

        for dev in self.dev_map.values():
            dev.save(update_fields=dev.update_fields)

    def update_ds(self, ds):
        dev = ds.parent
        if dev.dev_ui not in self.dev_map:
            self.dev_map[dev.dev_ui] = dev

        self.update_health(ds, dev)

        now_ts = create_now_ts_ms()
        ds.health_next_eval_ts = now_ts + max(
            settings.TIME_DS_HEALTH_EVAL_MS, ds.time_update * settings.NEXT_EVAL_MARGIN_COEF
        )
        ds.update_fields.add("health_next_eval_ts")

        ds.save(update_fields=ds.update_fields)

    def update_health(self, ds, dev):
        now_ts = create_now_ts_ms()
        if ds.last_valid_reading_ts is None:
            if now_ts - ds.created_ts > ds.time_nd_health_error:  # TODO: from 'enabled' or from 'created'?
                nd_health = HealthGrades.ERROR
            else:
                nd_health = HealthGrades.UNDEFINED
        else:
            if now_ts - ds.last_valid_reading_ts > ds.time_nd_health_error:
                nd_health = HealthGrades.ERROR
            else:
                nd_health = HealthGrades.OK
        set_attr_if_cond(nd_health, "!=", ds, "nd_health")

        health = max(ds.msg_health, ds.nd_health)
        if not set_attr_if_cond(health, "!=", ds, "health"):
            return

        logger.debug(f"Ds {ds.pk} {ds.name}: health changed to {health}")

        enqueue_update(dev, now_ts)
        logger.debug(
            f"Enqueue parent device {dev.pk} '{dev.name}' update for {
                create_iso_str_from_ts_ms(dev.next_upd_ts)
                }"
        )
