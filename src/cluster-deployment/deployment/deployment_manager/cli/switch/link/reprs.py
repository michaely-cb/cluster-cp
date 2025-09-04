import functools
from dataclasses import dataclass, field
from typing import Literal, Optional, Tuple, List

from deployment_manager.db.models import Link
from deployment_manager.tools.switch.utils import interface_sort_key
from deployment_manager.tools.utils import ReprFmtMixin

@dataclass
class LinkRepr(ReprFmtMixin):
    src_name: str
    src_if: str
    dst_name: str
    dst_if: str = ""
    speed: int = 0
    origin: str = ""
    src_if_role: str = ""
    dst_if_role: str = ""

    @classmethod
    def from_link(cls, l: Link) -> 'LinkRepr':
        return cls(
            src_name=l.src_device.name, src_if=l.src_if, src_if_role=l.src_if_role,
            dst_name=l.dst_name, dst_if=l.dst_if, dst_if_role=l.dst_if_role,
            speed=l.speed, origin=l.origin
            )

    def to_dict(self) -> dict:
        return {
            "src_name": self.src_name, "src_if": self.src_if, "src_if_role": self.src_if_role,
            "dst_name": self.dst_name, "dst_if": self.dst_if, "dst_if_role": self.dst_if_role,
            "speed": self.speed, "origin": self.origin
            }

    def to_table_row(self) -> list:
        return [self.src_name, self.src_if, self.src_if_role, self.dst_name, self.dst_if, self.dst_if_role, self.speed, self.origin]

    @classmethod
    def table_header(cls) -> list:
        return ["src_name", "src_if", "src_if_role", "dst_name", "dst_if", "dst_if_role", "speed", "origin"]

    @functools.cached_property
    def sort_key(self) -> Tuple[str, str]:
        """ normalize the port numbering for better sort ordering """
        return self.src_name, interface_sort_key(self.src_if),


@dataclass(frozen=True)
class LinkStatusRepr(ReprFmtMixin):
    src: str
    dst: str
    src_up: Optional[bool]
    src_status: str
    dst_up: Optional[bool]
    dst_status: str

    def to_dict(self) -> dict:
        return {"src": self.src, "dst": self.dst,
                "src_up": self.src_up, "dst_up": self.dst_up,
                "src_status": self.src_status, "dst_status": self.dst_status}

    @functools.cached_property
    def sort_key(self) -> Tuple[str, str]:
        """ src from switch like NAME:PORT - normalize the port numbering for better sort ordering """
        parts = self.src.split(":", 1)
        if len(parts) == 1:
            return self.src, self.src,
        return parts[0], interface_sort_key(parts[1]),

    @staticmethod
    def from_link(l: Link) -> 'LinkRepr':
        return LinkRepr(l.src_device.name, l.src_if, l.dst_name, l.dst_if, l.speed)

    def to_table_row(self) -> list:
        return [self.src, self.dst, self.src_up, self.dst_up, self.src_status, self.dst_status]

    @classmethod
    def table_header(cls) -> list:
        return ["src", "dst", "src_up", "dst_up", "src_status", "dst_status"]

LINK_STATE_MATCH = "match"
LINK_STATE_MISSING = "missing"
LINK_STATE_MISMATCH = "mismatch"
LINK_STATE_UNRECOGNIZED = "unrecognized"
LINK_STATES = (LINK_STATE_MATCH, LINK_STATE_MISSING, LINK_STATE_MISMATCH, LINK_STATE_UNRECOGNIZED)

@dataclass
class LinkNeighStatusRepr(ReprFmtMixin):
    src: str
    src_if: str
    dst: str
    dst_if: str
    dst_matcher_type: str # LLDP, ETH 
    actual_dst: str
    actual_dst_if: str
    state: str 
    link_origin: str
    src_if_up: str = ""
    dst_if_up: str = ""
    comment: str = ""  # optional comment

    @functools.cached_property
    def sort_key(self) -> Tuple[str, str]:
        """ src from switch like NAME:PORT - normalize the port numbering for better sort ordering """
        return self.src, interface_sort_key(self.src_if),

    def to_table_row(self) -> list:
        link_dst = f"{self.dst}:{self.dst_if}" if self.dst or self.dst_if else ""
        actual_dst = f"{self.actual_dst}:{self.actual_dst_if}" if self.actual_dst or self.actual_dst_if else ""
        return [f"{self.src}:{self.src_if}", link_dst, actual_dst, self.state, self.dst_matcher_type, self.src_if_up, self.dst_if_up, self.link_origin, self.comment]

    @classmethod
    def table_header(cls) -> list:
        return ["src", "link_dst", "actual_dst", "state", "match_type", "src_up", "dst_up", "link_origin", "comment"]

@dataclass
class LinkDetectMacsRepr(ReprFmtMixin):
    dst_name: str
    dst_if: str
    match_state: Literal["MATCH", "MISMATCH", "MISSING", "NEW"]
    actual_macs: List[str] = field(default_factory=list)
    src_name: str = ""
    src_if: str = ""
    current_mac: str = ""
    link_state: str = ""

    def set_link_state(self, new_link_state: str):
        if self.match_state == "MISSING":
            self.link_state = new_link_state

    def to_dict(self) -> dict:
        if self.match_state == "MATCH":
            return {
                    "dst_name": self.dst_name, "dst_if": self.dst_if,
                    "current_mac": self.current_mac, "actual_macs": self.actual_macs,              
                    "match_state": self.match_state
                    }
        elif self.match_state == "MISMATCH":
            return {
                    "dst_name": self.dst_name, "dst_if": self.dst_if,
                    "current_mac": self.current_mac, "actual_macs": self.actual_macs,              
                    "match_state": self.match_state
                    }
        elif self.match_state == "MISSING":
            return {
                    "dst_name": self.dst_name, "dst_if": self.dst_if,
                    "src_name": self.src_name, "src_if": self.src_if,
                    "link_state": self.link_state,
                    "match_state": self.match_state
                    }
        else:
            return {
                    "dst_name": self.dst_name, "dst_if": self.dst_if,
                    "actual_macs": self.actual_macs,
                    "match_state": self.match_state
                    }

    def to_table_row(self) -> list:
        return [self.dst_name, self.dst_if, 
                self.src_name, self.src_if, 
                self.current_mac, ",".join(self.actual_macs), self.link_state, 
                self.match_state, "True" if self.match_state in ("NEW", "MISMATCH") else "False"]

    @classmethod
    def table_header(cls) -> list:
        return ["dst_name", "dst_if", "src_name", "src_if", "current_mac", "actual_macs", "link_state", "match_state", "update"]