import logging
from django.db import transaction
from django.conf import settings

from apps.assets.models import Asset
from utils.ts_utils import create_now_ts_ms
from utils.update_utils import update_func_by_property_map, update_reeval_fields, set_attr_if_cond
from common.constants import reeval_fields

logger = logging.getLogger("#asset_updater")


def print_tree(tree, str_arr, offset=0):
    for node in tree:
        str_arr.append(f"{'-' * offset}{node.name}")
        if hasattr(node, "children"):
            print_tree(node.children, str_arr, offset + 1)

    return "\n".join(str_arr)


class AssetUpdater:

    def execute(self):
        self.get_needing_update_assets()

        if len(self.needing_update_assets) == 0:
            return

        self.update_some_assets_from_list()

    def get_needing_update_assets(self):
        now_ts = create_now_ts_ms()
        self.needing_update_assets = Asset.objects.filter(
            next_upd_ts__lte=now_ts,
        ).order_by(
            "next_upd_ts"
        )[: settings.MAX_ASSETS_TO_UPD]
        if len(self.needing_update_assets) > 0:
            logger.debug("List of assets needing update")
            logger.debug(self.needing_update_assets)

    @transaction.atomic
    def update_some_assets_from_list(self):
        self.create_map_of_assets_to_update()
        self.build_tree()
        self.process_starting_from_leaves(self.tree)

    def create_map_of_assets_to_update(self):
        self.assets_to_update_map = {}
        for asset in self.needing_update_assets:
            asset_pk = None
            if asset is not None:
                asset_pk = asset.pk

            while asset_pk is not None:
                if asset_pk not in self.assets_to_update_map:
                    asset = (
                        Asset.objects.filter(pk=asset_pk)
                        .prefetch_related("assets")
                        .prefetch_related("applications")
                        .prefetch_related("devices")
                        .select_for_update()
                        .first()
                    )
                    self.assets_to_update_map[asset_pk] = asset
                else:
                    asset = self.assets_to_update_map[asset_pk]
                asset_pk = asset.parent_id

            if len(self.assets_to_update_map) > settings.MAX_ASSETS_TO_UPD:
                break

    def build_tree(self):
        self.tree = []
        for asset in self.assets_to_update_map.values():
            parent = None
            if asset.parent_id is not None:
                parent = self.assets_to_update_map[asset.parent_id]  # parent is bound to be in the map
            if parent is None:
                self.tree.append(asset)
            else:
                if hasattr(parent, "children"):
                    parent.children.append(asset)
                else:
                    parent.children = [asset]

        logger.debug(print_tree(self.tree, ["Tree:"]))

    def process_starting_from_leaves(self, nodes):
        # ensure the childrens are processed before their parents
        for node in nodes:
            if hasattr(node, "children") and len(node.children) > 0:
                self.process_starting_from_leaves(node.children)

            self.update_node(node)

    def update_node(self, asset):
        logger.debug(f"Asset {asset.pk} {asset.name}: update")
        children = [
            *asset.applications.all(),
            *asset.devices.all(),
            *asset.assets.all(),
        ]

        for field_name in asset.reeval_fields:
            self.update_asset_field(asset, field_name, children)

        if asset.parent_id is not None:
            parent = self.assets_to_update_map[asset.parent_id]  # parent is bound to be in the map

            # Special update mode.
            # If an asset has all 3 reeval fields to recalculate (which usually happens after the bulk update),
            # then the parent will also be forced to reevaluate all 3 reeval fields.
            # It will cause a chain reaction up to the top of the asset tree.
            if len(asset.reeval_fields) == 3:
                update_reeval_fields(parent, reeval_fields)

            logger.debug(f"Parent 'Asset {parent.pk}' reeval fields -> {parent.reeval_fields}")

            # this trick is needed as 'a' is completely different from 'asset'
            qs = parent.assets.all()
            for a in qs:
                if a.pk == asset.pk:
                    a.curr_state = asset.curr_state
                    a.status = asset.status
                    a.health = asset.health

        asset.reeval_fields = []  # reset reeval fields
        asset.update_fields.add("reeval_fields")

        asset.next_upd_ts = settings.MAX_TS_MS
        asset.update_fields.add("next_upd_ts")

        asset.save(update_fields=asset.update_fields)

    def update_asset_field(self, asset, field_name, children):
        func = update_func_by_property_map[field_name]
        old_value = getattr(asset, field_name)
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

        logger.debug(f"Asset {asset.pk} '{asset.name}': {field_name} changed from {old_value} to {new_value}")

        if asset.parent_id is None:
            return

        parent = self.assets_to_update_map[asset.parent_id]  # parent is bound to be in the map

        update_reeval_fields(parent, field_name)
