#!/bin/env python3
""" Dashboard template helper.
    Invoke as ./monitoring-dashboard-helper.py OPERATION infile.json >outfile.json
    Where OPERATION:=stamplock-resdb|sessionlock-resdb|overview|session|job
"""

import json
import os
import re
import sys

def find_url(root):
    if isinstance(root, dict):
        for k,v in root.items():
            if k == "url" and isinstance(v, str):
                yield root, k
            else:
                yield from find_url(v)
    elif isinstance(root, list):
        for i in root:
            yield from find_url(i)
            
if len(sys.argv) != 3:
    print(f"Invalid arguments. Invoke as: ./{sys.argv[0]} [stamplock-resdb|sessionlock-resdb|overview|session|job] filename", file=sys.stderr)
    sys.exit(1)


fn = os.path.basename(sys.argv[2])

# encoding needed for jenkins
with open(sys.argv[2], encoding="utf8") as fp:
    db = json.load(fp)

if sys.argv[1] == "stamplock-resdb": #for res db
    # drop -internal from UID
    if re.match(r'.*-internal', db['uid']):
        db['uid'] = re.sub(r'(.*)-internal', r'\1-stamp', db['uid'])
    else:
        print("Trying to stamplock a resdb but it didn't end in -internal", file=sys.stderr)
        sys.exit(1)

    found_var = False
    for tvar in db['templating']['list']:
        if tvar['name'] == "node":
            tvar['definition'] = tvar['query']['query'] = "label_values(stamp_config{stamp=~\"$stamp\"},node)"
            found_var = True
            break
        elif tvar['name'] == "switch":
            tvar['definition'] = tvar['query']['query'] = "label_values(stamp_config{stamp=~\"$stamp\"},switch)"
            found_var = True
            break
        elif tvar['name'] == "system":
            tvar['definition'] = tvar['query']['query'] = "label_values(stamp_config{stamp=~\"$stamp\"},system)"
            found_var = True
            break

    if not found_var:
        print("Trying to fixup a res dashboard but didn't find the expected device variable (node/system/switch)", file=sys.stderr)
        sys.exit(1)

    db['templating']['list'].append({ "hide": 2,
                                  "name": "stamp",
                                  "options": [],
                                  "query": "",
                                  "type": "custom" })
    

    
    for d,k in find_url(db):
        d[k] = re.sub(r'(/?d/)(.*)-internal/(.*\?.*)?', r'\1\2-stamp/\3', d[k])

elif sys.argv[1] == "sessionlock-resdb":
    # drop -internal from UID
    if re.match(r'.*-internal', db['uid']):
        db['uid'] = re.sub(r'(.*)-internal', r'\1-session', db['uid'])
    else:
        print("Trying to stamplock the overviewdb but it didn't end in -internal", file=sys.stderr)
        sys.exit(1)

    found_var = False
    for tvar in db['templating']['list']:
        if tvar['name'] == "node":
            tvar['definition'] = tvar['query']['query'] = "query_result(node:namespace_group_role_info{namespace=\"$session\"})"
            tvar['regex'] = '/node="(?<text>[^"]+)/g'
            found_var = True
            break
        elif tvar['name'] == "switch":
            tvar['definition'] = tvar['query']['query'] = "query_result(group by (switch_id) ((label_replace(system_label{namespace=\"$session\"}, \"node\", \"$1\", \"system\", \"(.*)\") or on (node) node:namespace_group_role_info{namespace=\"$session\"}) * on (node) group_right group by (node, switch_id) (interface_watch_list)))"
            tvar['regex'] = '/switch_id="(?<text>[^"]+)/g'
            found_var = True
            break
        elif tvar['name'] == "system":
            tvar['definition'] = tvar['query']['query'] = "label_values(system_label{namespace=\"$session\"},system)"
            found_var = True
            break

    if not found_var:
        print("Trying to fixup a res dashboard but didn't find the expected device variable (node/system/switch)", file=sys.stderr)
        sys.exit(1)

    db['templating']['list'].append({ "hide": 2,
                                  "name": "session",
                                  "options": [],
                                  "query": "",
                                  "type": "custom" })
    

    
    for d,k in find_url(db):
        d[k] = re.sub(r'(/?d/)(.*)-internal/(.*\?.*)?', r'\1\2-session/\3', d[k])

elif sys.argv[1] == "overview":
    # drop -internal from UID
    if re.match(r'.*-internal', db['uid']):
        db['uid'] = re.sub(r'(.*)-internal', r'\1-stamp', db['uid'])
    else:
        print("Trying to stamplock the overview db but it didn't end in -internal", file=sys.stderr)
        sys.exit(1)

    found_var = False
    for idx, tvar in enumerate(db['templating']['list']):
        if tvar['name'] == 'available_stamps':
            found_var = True
            db['templating']['list'][idx] = {
                "current": {
                  "text": [
                    "All"
                  ],
                  "value": [
                    "$__all"
                  ]
                },
                "definition": "label_values(stamp_publish_config,stamp)",
                "hide": 2,
                "includeAll": True,
                "multi": True,
                "name": "available_stamps",
                "options": [],
                "query": {
                  "qryType": 1,
                  "query": "label_values(stamp_publish_config,stamp)",
                  "refId": "PrometheusVariableQueryEditor-VariableQuery"
                },
                "refresh": 2,
                "regex": "",
                "type": "query"
            }
            break
        
    if not found_var:
        print("Trying to fixup a res dashboard but didn't find the expected device variable (node/system/switch)", file=sys.stderr)
        sys.exit(1)

    for d,k in find_url(db):
        if '?' in d[k]:
            d[k] = re.sub(r'(/?d/)(.*)-internal/(.*\?.*)?', r'\1\2-stamp/\3&var-stamp=${stamp:pipe}', d[k])
        else:
            d[k] = re.sub(r'(/?d/)(.*)-internal/(.*\?.*)?', r'\1\2-stamp/\3', d[k])

elif sys.argv[1] == "session":
    # drop -internal from UID
    if re.match(r'.*-internal', db['uid']):
        db['uid'] = re.sub(r'(.*)-internal', r'\1-session', db['uid'])
    else:
        print("Trying to stamplock the session db but it didn't end in -internal", file=sys.stderr)
        sys.exit(1)

    # remote access script will lock down the session var
    for d,k in find_url(db):
        d[k] = re.sub(r'(/?d/)(.*)-internal/(.*\?.*)?', r'\1\2-session/\3', d[k])
        d[k] = re.sub(r'/d/WebHNShVz/wsjob-dashboard\?', r'/d/da2a4b2a-1ff8-4cc5-8b40-98ec06fa26a2/ml-user\?', d[k])

elif sys.argv[1] == "job":
    # drop -internal from UID
    if re.match(r'.*-internal', db['uid']):
        db['uid'] = re.sub(r'(.*)-internal', r'\1-session', db['uid'])
    else:
        print("Trying to stamplock a job db but it didn't end in -internal", file=sys.stderr)
        sys.exit(1)

    # remote access script will lock down the namespace var
    for d,k in find_url(db):
        d[k] = re.sub(r'(/?d/)(.*)-internal/(.*\?.*)?', r'\1\2-session/\3', d[k])

json.dump(db, sys.stdout, indent=2)

