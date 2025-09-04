import redfish
from unittest.mock import Mock

from deployment_manager.tools.pb1.ipmi import DellIPMI


def test_dell_boot_order():
    rfmock = Mock(spec=redfish.rest.v1.HttpClient)
    respmock = Mock(spec=redfish.rest.v1.RestResponse)
    respmock.dict = {"Members": [
        {"@odata.id": "/bios/HardDisk.List.1-1"},
        {"@odata.id": "/bios/NIC.1-1"},
        {"@odata.id": "/bios/HardDisk.List.1-2"},
        {"@odata.id": "/bios/NIC.Embedded.1-1"},
    ]}
    respmock.status = 200
    ipmi = DellIPMI("mock", "mock", "mock", rfmock)
    rfmock.get.return_value = respmock
    order = ipmi._create_boot_order()
    assert order == "HardDisk.List.1-1,HardDisk.List.1-2,NIC.Embedded.1-1,NIC.1-1"
