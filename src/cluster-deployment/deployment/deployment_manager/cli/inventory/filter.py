from django.db.models import Model, QuerySet, Q
from typing import List, Optional, Type
import re

query_re = re.compile(r"^(?P<lhs>\w+(.\w+)?)(?P<op>=~?|![=~])(?P<rhs>.*)$")

FIELD_MAP = {
    "elevation_name": "elevation_entity__name",
}

class Filter:

    @staticmethod
    def filter_devices(
        entity_class: Type[Model],
        args,
        query_set: QuerySet,
    ) -> QuerySet:

        if not args.filter:
            args.filter = []

        for f in args.filter:
            query_set = Filter.process_filter(entity_class, f, query_set)

        return query_set

    @staticmethod
    def process_filter(entity_class: Type[Model], f: str, query_set: QuerySet) -> QuerySet:
        m = query_re.match(f)
        if not m:
            raise ValueError(f"Invalid filter {f}. Must match expression {query_re.pattern}")

        m = m.groupdict()
        lhs, op, rhs = m['lhs'], m['op'], m['rhs']

        negative_search = "!" in op
        regex_search = "~" in op

        filter_prop = lhs
        filter_attr = ""

        if "." in lhs:
            filter_prop, filter_attr = lhs.split(".", 1)

        mapped_field = FIELD_MAP.get(filter_prop)

        if mapped_field:
            orm_field = mapped_field
            if filter_attr:
                orm_field += f"__{filter_attr}"
        elif filter_prop in [f.name for f in entity_class._meta.get_fields()]:
            orm_field = filter_prop
            if filter_attr:
                orm_field += f"__{filter_attr}"
        elif filter_attr:
            orm_field = f"properties__{filter_prop}__{filter_attr}"
        else:
            orm_field = f"properties__{filter_prop}"

        if regex_search:
            orm_field += "__regex"

        q = Q(**{orm_field: rhs})
        return query_set.filter(~q) if negative_search else query_set.filter(q)
