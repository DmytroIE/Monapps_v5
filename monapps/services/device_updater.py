import logging
from django.db import transaction
from django.conf import settings

from apps.devices.models import Device
from utils.ts_utils import create_now_ts_ms
from utils.update_utils import (
    derive_health_from_children,
    enqueue_update,
    update_reeval_fields,
    set_attr_if_cond
)

logger = logging.getLogger("#dev_updater")


class DeviceUpdater:
    def __init__(self):
        self.parent_map = {}

    @transaction.atomic
    def execute(self):
        self.now_ts = create_now_ts_ms()
        device_qs = (
            Device.objects.filter(
                next_upd_ts__lte=self.now_ts,
            )
            .order_by("next_upd_ts")
            .prefetch_related("parent")
            .prefetch_related("datastreams")
            .select_for_update()[: settings.MAX_DEVICES_TO_UPD]
        )

        if len(device_qs) == 0:
            return

        logger.debug(f"There are {len(device_qs)} devices to update")

        for dev in device_qs:
            self.update_device(dev)

        for parent in self.parent_map.values():
            parent.save(update_fields=parent.update_fields)

    def update_device(self, dev):
        parent = dev.parent
        if parent is not None:
            if parent.name not in self.parent_map:
                self.parent_map[parent.name] = parent

        children = list(dev.datastreams.filter(is_enabled=True))

        self.update_device_health(dev, children, parent)

        # move the update time several hours ahead
        dev.next_upd_ts = self.now_ts + settings.TIME_DELAY_ASSET_MANDATORY_UPDATE_MS
        dev.update_fields.add("next_upd_ts")

        dev.save(update_fields=dev.update_fields)

    def update_device_health(self, dev, children, parent):
        now_ts = create_now_ts_ms()
        chld_health = derive_health_from_children(children)
        set_attr_if_cond(chld_health, "!=", dev, "chld_health")
        health = max(dev.msg_health, dev.chld_health)

        if not set_attr_if_cond(health, "!=", dev, "health"):
            return

        logger.debug(f"Device {dev.pk} {dev.name}: health changed to {health}")

        if parent is None:
            return

        logger.debug(f"Enqueue parent 'asset {parent.pk}' update")
        update_reeval_fields(parent, "health")
        enqueue_update(parent, now_ts)
        logger.debug(f"Update enqueued for {parent.next_upd_ts}")
