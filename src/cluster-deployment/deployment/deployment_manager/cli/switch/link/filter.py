import argparse
import textwrap
from typing import List

from django.db.models import Q

from deployment_manager.cli.filter import query_re
from deployment_manager.db.models import QuerySet


def add_filter_arg(parser: argparse.ArgumentParser):
    parser.add_argument('--filter', '-f',
                        help=textwrap.dedent(
                            """
                            Filter conditions, allowed fields: src, dst, name, speed. `name` is src or dst name
                                operators: =, !=, =~, !~  
                                examples: -f src=~net1 dst!~ax filters for src containing net1 and dst not containing ax
                            """),
                        nargs="+",
                        required=False)


def apply_src_dst_filters(q: QuerySet, args: argparse.Namespace) -> QuerySet:
    """ parse NAME:INTERFACE? style filters, apply to query set """
    src_filter = getattr(args, "src", None)
    if src_filter:
        src_name, src_if = src_filter, None
        if ":" in src_filter:
            src_name, src_if = src_filter.split(":", 1)
            q = q.filter(src_device__name=src_name, src_if=src_if)
        else:
            q = q.filter(src_device__name=src_name)

    dst_filter = getattr(args, "dst", None)
    if dst_filter:
        dst_name, dst_if = dst_filter, None
        if ":" in dst_filter:
            dst_name, dst_if = dst_filter.split(":", 1)
            q = q.filter(dst_name=dst_name, dst_if=dst_if)
        else:
            q = q.filter(dst_name=dst_name)

    return q


def apply_filter(query_set: QuerySet, f: str) -> QuerySet:
    m = query_re.match(f)
    if not m:
        raise ValueError(f"invalid filter {f}. Must match expression {query_re.pattern}")

    m = m.groupdict()
    lhs, op, rhs = m['lhs'].lower(), m['op'], m['rhs']

    negative_search = "!" in op
    regex_search = "~" in op
    if lhs not in ("name", "src", "dst", "speed", "origin"):
        raise ValueError(f"invalid filter {f}. Supported fields: name, src, dst, speed, origin")

    if lhs == "name":
        # search over source or dest name
        if regex_search:
            or_clause = [{"src_device__name__regex": rhs}, {"dst_name__regex": rhs}]
        else:
            or_clause = [{"src_device__name": rhs}, {"dst_name": rhs}]
        q = Q()
        for clause in or_clause:
            q |= Q(**clause)
        return query_set.filter(~q) if negative_search else query_set.filter(q)
    else:
        lhs = {"src": "src_device__name", "dst": "dst_name"}.get(lhs, lhs)
        if regex_search:
            lhs += "__regex"
        query = Q(**{lhs: rhs})
        return query_set.filter(~query) if negative_search else query_set.filter(query)


def apply_filters(query_set: QuerySet, f: List[str]) -> QuerySet:
    """ Allow a simplified query filter over links """
    if not f:
        return query_set
    for field in f:
        query_set = apply_filter(query_set, field)
    return query_set
