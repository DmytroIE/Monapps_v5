from typing import Any, TypeVar
from collections.abc import Iterable


def find_max_ts(instances: Iterable[Any], ts_key: str = "time") -> int:
    m = 0
    for instance in instances:
        v = getattr(instance, ts_key)
        if v is not None and v > m:
            m = v
    return m


T = TypeVar("T")


def find_instance_with_max_attr(instances: Iterable[T], attr: str = "time") -> T | None:
    instance_with_max = None
    m = float("-inf")
    for instance in instances:
        v = getattr(instance, attr)
        if v is not None and v > m:
            m = v
            instance_with_max = instance
    return instance_with_max


def get_list_of_one_attr(instances: Iterable[Any], key: str = "time") -> list[Any]:
    return [getattr(instance, key) for instance in instances]


def replace_str_keys_with_int(str_key_dict) -> dict[int, Any]:
    int_key_dict = {}
    for k, v in str_key_dict.items():
        int_key_dict[int(k)] = v
    return int_key_dict
