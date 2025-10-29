def get_parent_full_id(instance):
    if hasattr(instance, "parent_id") and instance.parent_id is not None:
        return f"{instance._meta.get_field('parent').remote_field.model._meta.model_name} {instance.parent_id}"
    else:
        return None


# 'full id' looks like "datastream 125" and is needed to build the tree in the React 'tree' component
# (this 'tree' component needs an 'id' attribute to build the tree properly).
def get_instance_full_id(instance):
    return f"{instance._meta.model_name} {instance.id}"
