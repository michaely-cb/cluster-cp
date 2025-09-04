import datetime
import logging
import os
import yaml

from aiohttp import web

from generate_interface_list import parse_network_json

NETJSON_PATH = "/config/network_config.json.gz"
PUBLISHED_STAMP_PATH = "/config/published_stamps"

interface_watch_list = []
iwl_errors = []
stamp_config = []
last_json_mtime = -1


async def handle_netjsongz(request):
    global interface_watch_list
    global iwl_errors
    global stamp_config
    global last_json_mtime
    try:
        new_json_mtime = os.stat(NETJSON_PATH).st_mtime
    except:
        t = datetime.datetime.fromtimestamp(new_json_mtime).strftime("%Y%m%d%H%M%S")
        logging.error(
            f"Could not read the network JSON file at time {t}, serving old interface_watch_list if possible"
        )
        new_json_mtime = 0

    if new_json_mtime > last_json_mtime:
        last_json_mtime = new_json_mtime
        t = datetime.datetime.fromtimestamp(new_json_mtime).strftime("%Y%m%d%H%M%S")
        try:
            (_, _, interface_watch_list, iwl_errors, stamp_config, *_) = parse_network_json(NETJSON_PATH)
            logging.info(f"Using a new network json file at time {t}")
        except Exception as e:
            logging.error(f"Could not parse new network json file at time {t}, serving old or empty "
                          f"interface_watch_list: {e}")


    ps_metric = []
    try:
        with open(PUBLISHED_STAMP_PATH) as f:
            for l in f.readlines():
                l = l.strip()
                if l:
                    ps_metric.append(f"stamp_publish_config{{stamp=\"{l}\"}} 1")
    except FileNotFoundError:
        ps_metric = ["stamp_publish_config{{stamp=\"default\"}} 1"]
    except Exception as e:
        logging.error(f"Could not read the published stamp config: {e}")

    return web.Response(text="\n".join(interface_watch_list + iwl_errors + stamp_config + ps_metric))


app = web.Application()
app.add_routes([web.get('/interface_watch_list', handle_netjsongz)])

if __name__ == '__main__':
    web.run_app(app)
