from deployment_manager.tools.switch.templates import DataSwitchZTPTemplate, MgmtSwitchZTPTemplate
import pytest


def test_ztp_template_search():
    # should fuzzy match to "7060X6" in the template path
    template = DataSwitchZTPTemplate(vendor="AR", model="7060x6-dcs-f")
    assert "7060X6" in str(template.template_path)

    # should match to vendor
    template = DataSwitchZTPTemplate(vendor="AR", model="123abc")
    assert "AR/data_switch_ztp_template.txt" in str(template.template_path)

    # should match to vendor
    template = MgmtSwitchZTPTemplate(vendor="DL")
    assert "DL/mgmt_switch_ztp_template.txt" in str(template.template_path)

    # shouldn't match
    with pytest.raises(ValueError) as exc:
        DataSwitchZTPTemplate(vendor="EC", model="123abc")
    assert "does not have a ZTP template file" in str(exc.value)