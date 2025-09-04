import logging
import requests

logger = logging.getLogger(__name__)

# This is set short since we frequently change DNS records during maintenance
# Ideally it would be longer for efficiency. I tried using the /cache/flush API
# on __exit__ but it didn't seem to do anything.
DEFAULT_TTL = 60

class PowerDNSZoneManager:
    """
    Update DNS records in PowerDNS with a local cache. Note: this should be used with the powerDNSAdmin API which
    proxies the powerdns API. This allows fine-grained authz. You should configure the zone to have
    automatic creation of PTR records option set.
    See https://github.com/PowerDNS-Admin/PowerDNS-Admin/blob/master/docs/API.md
    """

    def __init__(self, api_url: str, api_key: str, domain: str):
        self._api_url = api_url.rstrip('/')
        self._api_key = api_key
        self._zone_api = f"{self._api_url}/api/v1/servers/localhost/zones"
        self.domain = domain
        self._cache = {
            "A": {},
            "CNAME": {},
        }

    def __enter__(self):
        self._populate_cache()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def _populate_cache(self):
        """ cache A, CNAME records as well as zones """
        response = requests.get(
            f"{self._zone_api}/{self.domain}",
            headers={'X-API-Key': self._api_key}
        )
        if response.status_code == 404:
            # we could auto-create the zone but since this is one-time critical set up, probably better manual anyways
            raise RuntimeError(f"zone for {self.domain} was not found in {self._api_url}. You must create the zone in "
                                "powerdns. Please set auto-creation of PTR records when you create the zone")
        response.raise_for_status()
        zone_data = response.json()
        for record in zone_data.get('rrsets', []):
            nrecords = len(record.get("records", []))
            if nrecords == 0:
                continue
            elif nrecords > 1:
                logger.warning(f"more than one record found for name '{record['name']}', ignoring additional records")

            cache = self._cache.get(record["type"], None)
            if cache is None:
                continue
            if record.get("ttl", -1) != DEFAULT_TTL:  # force update of differing TTL
                continue

            cache[record["name"]] = record["records"][0]["content"]

    def _canonicalize(self, name: str) -> str:
        name = name.lower()
        if "." in name:
            return name
        return f"{name}.{self.domain}."

    def add_a_record(self, name: str, addr: str, ttl=DEFAULT_TTL) -> bool:
        """ Add record, return True if server updated or False if record already existed """
        name = self._canonicalize(name)
        cache = self._cache["A"]
        if cache.get(name) == addr:
            return False

        if name in cache:
            self._update_zone(name, "A", addr, action="delete")
        self._update_zone(name, "A", addr, ttl, action='add')
        return True

    def remove_a_record(self, name: str) -> bool:
        """ Remove record, return True if server updated or False if record already existed """
        name = self._canonicalize(name)
        cache = self._cache["A"]
        if name not in cache:
            return False

        self._update_zone(name, "A", action='delete')
        return True

    def add_cname_record(self, name: str, fqdn: str, ttl=DEFAULT_TTL) -> bool:
        """ Add record, return True if server updated or False if record already existed """
        name = self._canonicalize(name)
        fqdn = self._canonicalize(fqdn)
        cache = self._cache["CNAME"]
        if cache.get(name) == fqdn:
            return False

        if name in cache:
            self._update_zone(name, "CNAME", name, action="delete")

        self._update_zone(name, "CNAME", fqdn, ttl, action='add')
        return True

    def remove_cname_record(self, name) -> bool:
        """ Remove record, return True if server updated or False if record already existed """
        name = self._canonicalize(name)
        cache = self._cache["CNAME"]
        if name not in cache:
            return False

        self._update_zone(name, "CNAME", action='delete')
        return True

    def _update_zone(self, name, record_type, content=None, ttl=DEFAULT_TTL, action='add', domain=None):
        if not domain:
            domain = self.domain

        rrset = {
            "name": name,
            "type": record_type,
            "ttl": ttl,
            "changetype": "REPLACE" if action == 'add' else "DELETE",
            "records": [{"content": content, "disabled": False}] if content else []
        }
        data = {"rrsets": [rrset]}

        response = requests.patch(
        f"{self._zone_api}/{domain}",
            headers={'X-API-Key': self._api_key},
            json=data,
        )
        if not response.ok and not (response.status_code == 404 and action == "delete"):
            logger.error(f"failed to PATCH {self._api_url} with {rrset} -> {response.text}")
            response.raise_for_status()
        logger.debug(f"PATCH {self._api_url} {response.status_code} - {action} {record_type} {name} {content or ''}")

        cache = self._cache.get(record_type)
        if cache:
            if action == 'add':
                cache[name] = content
            elif action == 'delete':
                cache.pop(name, None)
