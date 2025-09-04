import pytest

from deployment_manager.tools.utils import flat_map, parse_mac, from_duration, to_duration, strip_json_list


def test_flatmap():
    v = flat_map([["1", ["2", "33"], [], (()), "4"], "5"])
    assert v == ["1", "2", "33", "4", "5"]

    v = set(flat_map([[1, {2, 3}, [], (()), 4], [{5}, set()], set()]))
    assert v == {1, 2, 3, 4, 5}


def test_parse_mac():
    invalids = ["aa.bb.cc.dd.ee.gg", "aabbccddee"]
    for invalid in invalids:
        with pytest.raises(ValueError):
            parse_mac(invalid)

    valids = {"aabbccddeeff": "aa:bb:cc:dd:ee:ff", "aabb.ccdd.eeff": "aa:bb:cc:dd:ee:ff"}
    for valid, res in valids.items():
        assert parse_mac(valid) == res


def test_duration():
    durations = {
        "59s": 59,
        "1m31s": 91,
        "3h30m": 12600,
        "1d3h": 97200,
        "33d19h": 2919600,
    }

    for human, sec in durations.items():
        assert to_duration(sec) == human
        assert from_duration(human) == sec


def test_strip_json():
    valid = [
        """xxx$ show json
        [{},{},{},{}]
        """,
        "[{},{},{},{}]",
        """xxx$ show json
        [{},{},{},{}]
        xxx [marc] #"""
    ]
    for doc in valid:
        v = strip_json_list(doc)
        assert v == [{}, {}, {}, {}]

    invalid = [
        """$ show json [
        {},""",
        "{}"
    ]
    for doc in invalid:
        with pytest.raises(ValueError) as e:
            strip_json_list(doc)