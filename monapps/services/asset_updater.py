import logging
from django.db import transaction
from django.conf import settings

from apps.assets.models import Asset
from utils.ts_utils import create_now_ts_ms
from utils.update_utils import update_func_by_property_map, enqueue_update, update_reeval_fields, set_attr_if_cond
from utils.db_field_utils import get_instance_full_id
from common.constants import reeval_fields

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

        logger.debug(f"There are {len(asset_qs)} assets to update")

        asset_map = {}
        for asset in asset_qs:
            asset_full_id = get_instance_full_id(asset)
            if asset_full_id not in asset_map:
                asset_map[asset_full_id] = asset
        tree = self.create_asset_tree(asset_map)
        self.process_starting_from_leaves(tree)

    def create_asset_tree(self, asset_map):
        tree = []
        for asset in asset_map.values():
            if asset.parent is not None:
                if get_instance_full_id(asset.parent) not in asset_map:
                    # 'Root assets' are not assets on the top of the whole asset tree
                    # These are just parents of the assets on the top of the current tree, nothing more.
                    # For 'root' assets (that are not in the map), update will not happen in this iteration,
                    # but will be enqueued
                    asset.parent.root = True
                    tree.append(asset.parent)
                asset.root = False
                if hasattr(asset.parent, "children"):
                    asset.parent.children.append(asset)
                else:
                    asset.parent.children = [asset]
            else:
                asset.root = False
                tree.append(asset)

        return tree

    def process_starting_from_leaves(self, nodes):
        for node in nodes:
            if hasattr(node, "children") and len(node.children) > 0:
                self.process_starting_from_leaves(node.children)
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

        logger.debug(f"Asset {asset.pk} {asset.name}: update")
        logger.debug(f"Asset {asset.pk} {asset.name}: to be reevaluated: {asset.reeval_fields}")

        for field_name in asset.reeval_fields:
            self.update_asset_field(asset, field_name, children)

        # Handling the special mode.
        # If the asset has all 3 reeval fields (which usually happens after the bulk update)
        # - see next several lines), then the parent will update all reeval fields.
        # The same will do the parent of the parent etc.
        if len(asset.reeval_fields) == 3:
            update_reeval_fields(asset.parent, reeval_fields)

        asset.reeval_fields = []  # reset reeval fields
        asset.update_fields.add("reeval_fields")

        # Mandatory update - move the update time several hours ahead
        asset.next_upd_ts = self.now_ts + settings.TIME_DELAY_ASSET_MANDATORY_UPDATE_MS
        asset.update_fields.add("next_upd_ts")

        asset.save(update_fields=asset.update_fields)

    def update_asset_field(self, asset, field_name, children):
        func = update_func_by_property_map[field_name]
        logger.debug(f"Old value of {field_name}: {getattr(asset, field_name)}")
        new_value = func(children)
        logger.debug(f"New value of {field_name}: {new_value}")

        if not set_attr_if_cond(new_value, "!=", asset, field_name):
            return

        now_ts = create_now_ts_ms()
        if field_name == "status":
            asset.last_status_update_ts = now_ts
            asset.update_fields.add("last_status_update_ts")
        if field_name == "curr_state":
            asset.last_curr_state_update_ts = now_ts
            asset.update_fields.add("last_curr_state_update_ts")

        logger.debug(f"Asset {asset.pk}: {field_name} changed to {new_value}")

        update_reeval_fields(asset.parent, field_name)
        if asset.parent is None:
            return

        logger.debug(f"Parent (asset {asset.parent.pk}): reeval fields {asset.parent.reeval_fields}")
