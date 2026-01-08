import logging
import json
import humps
from typing import Literal

from django.db import models
from django.conf import settings

from common.constants import reeval_fields
from utils.db_field_utils import get_instance_full_id
from utils.ts_utils import create_dt_from_ts_ms, create_now_ts_ms

# from services.alarm_log import add_to_alarm_log
from utils.update_utils import enqueue_update, update_reeval_fields
from services.mqtt_publisher import mqtt_publisher, publish_with_delay

logger = logging.getLogger("#abs_classes")


class PublishingOnSaveModel(models.Model):

    class Meta:
        abstract = True

    published_fields = set()
    name = "PublishingOnSaveModel instance"  # backup, if 'name' was forgotten to be defined in a subclass
    parent = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Update_fields is used to collect the names of the fields that were changed.
        # It will then be used in the 'save' method and reset.
        # To align with the Django 'save' method signature, this field should be
        # used explicitly in the 'save' method -> instance.save(update_fields=instance.update_fields)
        self.update_fields = set()

    # TODO: overload the 'delete' method as well

    def save(self, **kwargs):
        message_type = "c" if self.pk is None else "u"
        # checking if the parent was changed
        # https://gist.github.com/schinckel/1591723
        has_parent_changed = False
        old_parent = None
        if self.pk:
            old_version = self.__class__.objects.get(pk=self.pk)
            if old_version.parent != self.parent:
                has_parent_changed = True
                old_parent = old_version.parent

        # The program relies upon the 'update_fields' parameter (a set of names of the changed fields) in the 'kwargs'.
        # It means that this set should be updated whenever any field of an instance is changed in the code
        # and finally passed to the 'save' method.
        # When the 'save' method is called, the presence of this field in 'kwargs' helps to identify where the changes
        # came from and, based on this, to undertake some additional actions.
        # For example, if the 'update_fields' field is 'None', it means that the 'save' method was called from
        # the admin interface, and it is necessary to create an MQTT message of a certain shape
        # and enqueue a bulk/total parent update.
        # If it is an empty set, then the 'save' method will be omitted.
        # If it is a non-empty set, then an MQTT message with ONLY the changed fields will be created and published.
        # It should also be noted that it is necessary to use not an arbitrary set, but the 'update_fields' field
        # attached to the instance, because it is used in some auxiliary functions.
        # Moreover, the whole program is built assuming that this 'self.update_fields' field is present
        # in instances of the “PublishingOnSaveModel” class.
        # An instance "drags" this field with it through many transformations.
        # In the 'save' method, after saving, it gets reset and an empty set is ready for the next processing cycle.

        super().save(**kwargs)

        update_fields = kwargs.get("update_fields")
        update_fields_length = 0
        try:
            if update_fields is not None:
                update_fields_length = len(update_fields)
            else:
                update_fields_length = -1  # anything but not 0
        except Exception:
            pass
        if update_fields is not None and update_fields_length == 0:
            return

        if update_fields is not None:
            fields_to_publish = self.published_fields.intersection(update_fields)
            if len(fields_to_publish) > 0:
                self.publish_on_mqtt(fields_to_publish, message_type)
        else:
            logger.debug(f"{get_instance_full_id(self)}: Updating from the bulk 'save' method")
            self.publish_on_mqtt(set(), message_type)
            if self.parent is not None:
                self.total_parent_update(self.parent)

        if has_parent_changed:
            self.total_parent_update(old_parent)

        # reset after processing
        self.update_fields = set()

    def publish_on_mqtt(self, fields_to_publish: set, message_type: Literal["c", "u", "d"]):
        if mqtt_publisher is None or not mqtt_publisher.is_connected():
            return

        mqtt_pub_dict = self.create_mqtt_pub_dict(fields_to_publish, message_type)

        topic = f"procdata/{settings.MONAPP_INSTANCE_ID}/{self._meta.model_name}/{self.pk}"
        payload_str = json.dumps(mqtt_pub_dict)

        # delay to ensure that all changes are saved in the db
        publish_with_delay(topic, payload_str, qos=0, delay_ms=50)

        # add_to_alarm_log("INFO", "Changes published", instance=self)
        logger.info(f"{get_instance_full_id(self)}: Changes published on MQTT")

    def delete(self, using=None, keep_parents=False):
        # when deleting an instance, a bulk parent update
        # will be enqueued here
        children_with_cascade_delete = []
        for relation in self._meta.related_objects:
            if relation.on_delete.__name__ == "CASCADE":
                related_model = relation.related_model
                children_with_cascade_delete += list(related_model.objects.filter(parent=self))

        id = self.id
        del_result = super().delete(using, keep_parents)
        self.id = id
        self.publish_on_mqtt(set(), "d")
        if self.parent is not None:
            self.total_parent_update(self.parent)

        if self._meta.label not in del_result[1] or del_result[1][self._meta.label] == 0:  # no rows deleted
            return del_result

        for child in children_with_cascade_delete:
            if hasattr(child, "publish_on_mqtt"):
                child.publish_on_mqtt(set(), "d")

        return del_result

    def create_mqtt_pub_dict(self, fields_to_publish: set, message_type: Literal["c", "u", "d"]) -> dict:
        mqtt_pub_dict = {}
        mqtt_pub_dict["id"] = get_instance_full_id(self)
        mqtt_pub_dict["messageType"] = message_type

        for field in fields_to_publish:
            attr = getattr(self, field, None)
            camelized_field = humps.camelize(field)
            mqtt_pub_dict[camelized_field] = attr

        return mqtt_pub_dict

    def total_parent_update(self, parent):
        parent_full_id = get_instance_full_id(parent)
        logger.debug(f"{parent_full_id}: Updating parent from the bulk 'save' method")
        if hasattr(parent, "reeval_fields"):  # if parent is an asset, all 'reeval_fields' should be reevaluated
            update_reeval_fields(parent, reeval_fields)
            logger.debug(f"{parent_full_id}: To be reevaluated: {reeval_fields}")
        if hasattr(parent, "next_upd_ts"):
            enqueue_update(parent, create_now_ts_ms())
            logger.debug(f"{parent_full_id}: Update enqueued for {parent.next_upd_ts}")
        parent.save(update_fields=parent.update_fields)


class AnyDsReading(models.Model):
    class Meta:
        abstract = True

    short_name = ""

    pk = models.CompositePrimaryKey("datastream_id", "time")
    time = models.BigIntegerField()
    datastream = models.ForeignKey("datastreams.Datastream", on_delete=models.PROTECT)
    db_value = models.FloatField()

    @property
    def value(self) -> float | int:
        if self.datastream.is_value_interger:
            return int(self.db_value)
        else:
            return self.db_value

    @value.setter
    def value(self, value: float) -> None:
        if self.datastream.is_value_interger:
            self.db_value = round(value, 0)
        else:
            self.db_value = value

    def __str__(self):
        dt_str = create_dt_from_ts_ms(self.time).strftime("%Y/%m/%d %H:%M:%S")
        if self.datastream.is_value_interger:
            return f"{self.short_name} ds:{self.datastream.pk} ts:{dt_str} val: {self.value}"
        else:
            return f"{self.short_name} ds:{self.datastream.pk} ts:{dt_str} val: {self.value:.3f}"


class AnyNoDataMarker(models.Model):

    class Meta:
        abstract = True

    short_name = ""

    pk = models.CompositePrimaryKey("datastream_id", "time")
    time = models.BigIntegerField()
    datastream = models.ForeignKey("datastreams.Datastream", on_delete=models.PROTECT)

    def __str__(self):
        dt_str = create_dt_from_ts_ms(self.time).strftime("%Y/%m/%d %H:%M:%S.%f")
        return f"{self.short_name} ds:{self.datastream.pk} ts:{dt_str[:-3]}"
