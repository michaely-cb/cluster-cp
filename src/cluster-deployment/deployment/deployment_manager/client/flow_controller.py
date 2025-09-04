import contextlib
import dataclasses
import logging
import time
from typing import Tuple, List, Any, Generator

import requests
from requests import Session
from requests.auth import HTTPBasicAuth

from deployment_manager.tools.utils import ReprFmtMixin, ping_check

logger = logging.getLogger(__name__)

# Order of the config items is important. These are the orderings that the Belimo support team suggested.
_CONF_ENDPOINT_VALUE = [
    ("application/EnergyValve4/EnergyValve4/EnergyValve4/OcCouCMinimalSet/LcPlatformMinimalSet/InterfaceControlBlockInterface/SelectSetpointSource", "1"),
    ("application/EnergyValve4/EnergyValve4/EnergyValve4/WaterDeviceSettingsBlockInterface/SelectWaterControlMode", "1"),
    ("application/EnergyValve4/EnergyValve4/EnergyValve4/WaterFlowControlBlockInterface/SetMaximumWaterFlow", "0.00178"),
    ("application/EnergyValve4/EnergyValve4/EnergyValve4/WaterFlowControlBlockInterface/SetMinimumWaterFlow", "0.00161"),
    ("co_uc/poe/setpoesupply", "true"),
    ("units/web_unit_settings", "default/WaterFlow:litresPerMinute,default/Power:kilowatt,default/Energy:kilowattHour,default/Temperature:celsius"),
    ("application/EnergyValve4/EnergyValve4/EnergyValve4/WaterDeviceSettingsBlockInterface/SelectWaterMediumType", "0"),
    ("modbus/config/enabled", "true"),
    ("bus/selectbusprotocol", "5"),
    ("modbus/config/modbuswatchdogtimeout", "120"),
    ("modbus/config/tcp/port", "502"),
    ("modbus/config/tcp/keepopentimeouts", "60"),
    ("bus/busfailaction", "0"),
    # useful for opening the valve to a specific position
    ("application/EnergyValve4/EnergyValve4/EnergyValve4/SetpointBlockInterface/SetRelativeSetpoint", "70.0"),
]

_BASE_PATH = "api/v1/datapoints/"
_HOSTNAME_PATH = "application/EnergyValve4/EnergyValve4/EnergyValve4/DeviceIdentificationBlockInterface/SetDeviceDescription"
_RACK_PATH = "application/EnergyValve4/EnergyValve4/EnergyValve4/DeviceIdentificationBlockInterface/SetDeviceInstallationLocation"
_MIN_FLOW_PATH = "application/EnergyValve4/EnergyValve4/EnergyValve4/WaterFlowControlBlockInterface/SetMinimumWaterFlow"
_MAX_FLOW_PATH = "application/EnergyValve4/EnergyValve4/EnergyValve4/WaterFlowControlBlockInterface/SetMaximumWaterFlow"
_CUR_FLOW_PATH = "application/EnergyValve4/EnergyValve4/EnergyValve4/BasicWaterFlowSensorBlockInterface/AbsoluteWaterFlow"
_REL_POS_PATH = "application/EnergyValve4/EnergyValve4/EnergyValve4/BasicActuatorBlockInterface/RelativeActuatorPosition"
_OUT_TEMP_PATH = "application/EnergyValve4/EnergyValve4/EnergyValve4/BasicWaterFlowSensorBlockInterface/FlowBodyWaterTemperature"
_IN_TEMP_PATH = "application/EnergyValve4/EnergyValve4/EnergyValve4/BasicThermalEnergyMeterAdditionalsBlockInterface/RemoteWaterTemperature"
_COOLING_POWER_PATH = "application/EnergyValve4/EnergyValve4/EnergyValve4/PowerCalculationBlockInterface/AbsoluteCoolingPower"  # watts


@dataclasses.dataclass
class GetFlowRateRepr(ReprFmtMixin):
    name: str
    current_flow: float = 0.0  # current flow rate in L/min
    cooling_power_kw: float = 0.0 # cooling power in kW
    actuator_percent_open: float = 0.0  # current relative actuator position in %
    output_temp_c: float = 0.0  # output water temp in celcius
    input_temp_c: float = 0.0 # input water temp in celcius
    min_flow: float = 0.0
    max_flow: float = 0.0
    in_range: bool = False
    error: str = ""

    @classmethod
    def table_header(cls) -> list:
        return ["name", "in_range", "current_flow", "cooling_kw", "min_flow", "max_flow",
                "actuator_percent_open", "input_temp_c", "output_temp_c",
                "error"]

    def to_table_row(self) -> list:
        return [self.name, self.in_range,
                f"{self.current_flow:.4f}", f"{self.cooling_power_kw:.2f}",
                f"{self.min_flow:.4f}", f"{self.max_flow:.4f}",
                f"{self.actuator_percent_open:.2f}%",
                f"{self.input_temp_c:.2f}",
                f"{self.output_temp_c:.2f}",
                self.error]

    @property
    def sort_key(self) -> Tuple[str,]:
        return (self.name,)


class BelimoFlowController:
    """
    Some config client for Belimo valves. These are devices that control water flow in a system. These are generally
    managed by the datacenter, but on some sites, we need to ensure that the flow rates are set correctly

    See: https://cerebras.atlassian.net/wiki/spaces/SS/pages/4457791602/Belimo+Valve+Configuration+Process+for+OKC+Data+Center
    See: https://github.com/Cerebras/platform/blob/5b8ac15f56b31d4cf072c3cf64c5b62ff3d8f760/util/cdiag/tools/belimo_config.py#L275
    Query /api/v1/datapoints for full API endpoints
    """

    def __init__(self, host: str, username: str, password: str, ip=None, port=443):
        self.host = host
        self._addr = ip or host
        self._username = username
        self._password = password
        self._port = port
        self._base_url = f"https://{self._addr}:{self._port}/{_BASE_PATH}"
        self._verified_reachability = False

    @contextlib.contextmanager
    def _with_session(self) -> Generator[Session, Any, None]:
        # this prevents HTTP requests from taking a long time to initially timeout when the host cannot be reached
        if not self._verified_reachability:
            if not ping_check(self.host):
                raise RuntimeError(f"unable to reach {self.host}")
            self._verified_reachability = True

        session = requests.Session()
        session.auth = HTTPBasicAuth(self._username, self._password)
        session.verify = False
        session.timeout = (5, 10,)  # connect timeout, read timeout
        session.headers.update({
            "Content-Type": "application/json",
        })
        try:
            yield session
        finally:
            session.close()

    def _resolve_configs(self) -> List[Tuple[str, str]]:
        config = [
            (_HOSTNAME_PATH, self.host,),
            (_RACK_PATH, self.host.split("-")[0],),  # rack is derived from the hostname
            *_CONF_ENDPOINT_VALUE,
        ]
        return config

    def get_config_updates(self, params: List[Tuple[str, str]] = None) -> List[Tuple[str, str]]:
        """
        Check which configuration paths need to be updated based on the current device settings.
        """
        if params is None:
            params = self._resolve_configs()
        needs_update = []
        with self._with_session() as session:
            for path, value in params:
                response = session.get(f"{self._base_url}{path}")
                logger.debug(f"{self.host} GET {path} response: {response.status_code} {response.text}")
                response.raise_for_status()
                current_value = response.json().get("datapoints", [{"value": None}])[0].get("value")
                if current_value != value:
                    needs_update.append((path, value,))
        return needs_update

    def put_config(self, config_updates: List[Tuple[str, str]], retries: int = 1) -> None:
        """
        Apply configuration updates to the Belimo device with retry logic (observed that sometimes the device does not
        apply the updates correctly on the first try).
        """
        attempt = 0
        while attempt <= retries and config_updates:
            for path, update in config_updates:
                # need to open a new session for each PUT request for some reason...
                with self._with_session() as session:
                    response = session.put(f"{self._base_url}{path}", json={"value": update})
                    logger.debug(f"{self.host} PUT {path} response: {response.status_code} {response.text}")
                response.raise_for_status()
            time.sleep(2) # wait for the device to process the updates
            # check if the updates were applied correctly, if not retry those configs
            config_updates = self.get_config_updates(params=config_updates)
            attempt += 1

        if config_updates:
            msg = ",".join(path.split('/')[-1] for path, _ in config_updates)
            raise RuntimeError(
                f"failed to apply configs after {attempt} attempts, failures: {msg}"
            )

    def _get_raw_parameter(self, path: str) -> str:
        """ return raw data param """
        with self._with_session() as session:
            response = session.get(f"{self._base_url}{path}")
        logger.debug(f"{self.host} GET {path} response: {response.status_code} {response.text}")
        response.raise_for_status()
        value = response.json().get("datapoints", [{"value": None}])[0].get("value")
        if value is None:
            raise ValueError(f"Parameter not found at {path}")
        return value

    def _get_flow_parameter(self, path: str) -> float:
        """ return flow data param in L/min """
        value = self._get_raw_parameter(path)
        # m^3/s is returned, convert to L/min
        return float(value) * 60 * 1000

    def get_flow_rate(self) -> GetFlowRateRepr:
        """
        Verify that the current flow rate is within the configured min/max range.
        Returns a tuple (is_valid: bool, message: str)
        """
        def kelvin_to_celcius(i) -> float:
            return float(i) - 273.15

        r = GetFlowRateRepr(self.host)
        try:
            r.actuator_percent_open = float(self._get_raw_parameter(_REL_POS_PATH))
            r.current_flow = self._get_flow_parameter(_CUR_FLOW_PATH)
            r.cooling_power_kw = float(self._get_raw_parameter(_COOLING_POWER_PATH)) / 1000.0
            r.input_temp_c = kelvin_to_celcius(self._get_raw_parameter(_IN_TEMP_PATH))
            r.output_temp_c = kelvin_to_celcius(self._get_raw_parameter(_OUT_TEMP_PATH))
            r.min_flow = self._get_flow_parameter(_MIN_FLOW_PATH)
            r.max_flow = self._get_flow_parameter(_MAX_FLOW_PATH)
            r.in_range = r.min_flow <= r.current_flow <= r.max_flow
            return r
        except Exception as e:
            r.error = f"Unexpected {e.__class__.__name__}: {str(e)}"
            return r
