import logging
from django.db import transaction
from django.conf import settings

from apps.assets.models import Asset
from utils.ts_utils import create_now_ts_ms
from utils.update_utils import update_func_by_property_map, enqueue_update, update_reeval_fields, set_attr_if_cond
from utils.db_field_utils import get_instance_full_id

logger = logging.getLogger("#asset_updater")


class AssetUpdater:

    @transaction.atomic
    def execute(self):
        self.now_ts = create_now_ts_ms()
        asset_qs = (
            Asset.objects.filter(
                next_upd_ts__lte=self.now_ts,
            )
            .order_by("next_upd_ts")
            .prefetch_related("parent")
            .prefetch_related("assets")
            .prefetch_related("applications")
            .prefetch_related("devices")
            .select_for_update()[: settings.MAX_ASSETS_TO_UPD]
        )

        if len(asset_qs) == 0:
            return

        asset_map = {}
        for asset in asset_qs:
            asset_full_id = get_instance_full_id(asset)
            if asset_full_id not in asset_map:
                asset_map[asset_full_id] = asset
        logger.debug("create tree")
        tree = self.create_asset_tree(asset_map)
        logger.debug("process tree")
        self.process_starting_from_leaves(tree)

    def create_asset_tree(self, asset_map):
        tree = []
        for asset in asset_map.values():
            logger.debug(f"- process asset {asset.pk} {asset.name}")
            if asset.parent is not None:
                logger.debug("- - parent is not None")
                if get_instance_full_id(asset.parent) not in asset_map:
                    # for 'root' assets (that are not in the map) update will not happen in this iteration,
                    # but will be enqueued
                    asset.parent.root = True
                    tree.append(asset.parent)
                    logger.debug(
                        f"- - parent = asset {asset.parent.pk} {asset.parent.name} added as root to the tree"
                    )

                asset.root = False
                if hasattr(asset.parent, "children"):
                    asset.parent.children.append(asset)
                else:
                    asset.parent.children = [asset]

                logger.debug(f"- - asset {asset.pk} {asset.name} added as children to the tree")
            else:
                asset.root = False
                tree.append(asset)
                logger.debug(f"- - asset {asset.pk} {asset.name} added to the top of the tree")

        return tree

    def process_starting_from_leaves(self, nodes):
        for node in nodes:
            if hasattr(node, "children") and len(node.children) > 0:
                self.process_starting_from_leaves(node.children)
            logger.debug(f"process asset {node.pk} {node.name}")
            if node.root:
                self.update_root_node(node)
            else:
                self.update_node(node)

    def update_root_node(self, asset):
        logger.debug(f"- root asset {asset.pk} {asset.name}: - Update")
        if len(asset.reeval_fields) == 0:
            return
        logger.debug(f"- root asset {asset.pk} {asset.name}: - to be reevaluated: {asset.reeval_fields}")
        enqueue_update(asset, create_now_ts_ms())
        logger.debug(f"- root asset {asset.pk} {asset.name}: - Enqueue update for {asset.next_upd_ts}")

        asset.save(update_fields=asset.update_fields)

    def update_node(self, asset):
        children = [
            *asset.applications.all(),
            *asset.devices.all(),
            *asset.assets.all(),
        ]

        logger.debug(f"- asset {asset.pk} {asset.name}: - Update")

        for field_name in asset.reeval_fields:
            self.update_asset_field(asset, field_name, children)

        asset.reeval_fields = []  # reset
        asset.update_fields.add("reeval_fields")

        asset.next_upd_ts = self.now_ts + settings.TIME_DELAY_ASSET_MANDATORY_UPDATE_MS  # move the update time several hours ahead
        asset.update_fields.add("next_upd_ts")

        asset.save(update_fields=asset.update_fields)

    def update_asset_field(self, asset, field_name, children):
        func = update_func_by_property_map[field_name]
        new_value = func(children)

        if not set_attr_if_cond(new_value, "!=", asset, field_name):
            return

        now_ts = create_now_ts_ms()
        if field_name == "status":
            asset.last_status_update_ts = now_ts
            asset.update_fields.add("last_status_update_ts")
        if field_name == "curr_state":
            asset.last_curr_state_update_ts = now_ts
            asset.update_fields.add("last_curr_state_update_ts")

        logger.debug(f"- - asset {asset.pk} {asset.name}: {field_name} changed -> {new_value}")

        update_reeval_fields(asset.parent, field_name)
        if asset.parent is None:
            return

        logger.debug(f"- - parent's (asset {asset.parent.pk} {asset.parent.name}) reeval fields: {asset.parent.reeval_fields}")
