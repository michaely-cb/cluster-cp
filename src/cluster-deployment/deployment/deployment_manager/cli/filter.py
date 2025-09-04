import re

from django.db.models import Q, QuerySet

from deployment_manager.db.device_props import ALL_DEVICE_PROP_DICT
from deployment_manager.db.models import Device

query_re = re.compile(r"^(?P<lhs>\w+(.\w+)?)(?P<op>=~?|![=~])(?P<rhs>.*)$")


def process_filter(f: str, query_set: QuerySet) -> QuerySet:
    m = query_re.match(f)
    if not m:
        raise ValueError(f"invalid filter {f}. Must match expression {query_re.pattern}")

    m = m.groupdict()
    lhs, op, rhs = m['lhs'].lower(), m['op'], m['rhs']

    negative_search = "!" in op
    regex_search = "~" in op
    filter_prop, filter_attr = lhs, ""
    if "." in filter_prop:
        filter_prop, filter_attr = filter_prop.split(".", 2)

    # special handling for device_role/role and device_type/type
    if filter_attr == "" and (filter_prop == "type" or filter_prop == "role"):
        filter_prop = f"device_{filter_prop}"

    if Device.field_exists(filter_prop):
        filter = filter_prop + ('__regex' if regex_search else '')
        if negative_search:
            return query_set.filter(~Q(**{filter: rhs}))
        return query_set.filter(Q(**{filter: rhs}))
    else:
        if (
            filter_prop not in ALL_DEVICE_PROP_DICT
            or filter_attr not in ALL_DEVICE_PROP_DICT[filter_prop]
        ):
            raise ValueError(f"invalid filter {f}, unknown property")

        prop_key = "properties__property_value" + ('__regex' if regex_search else '')
        query = Q(
            **{
                "properties__property_name": filter_prop,
                "properties__property_attribute": filter_attr,
                prop_key: rhs,
            }
        )
        return (
            query_set.filter(~query)
            if negative_search
            else query_set.filter(query)
        )
