import pytest
from django.test.testcases import TestCase
from unittest.mock import Mock

from deployment_manager.flow.flow import SetSystemDcqcnFlow
from deployment_manager.flow.task import FlowContext, FlowStatus
from deployment_manager.tools.system import SystemCtl


@pytest.mark.skip
class SystemDcqcnFlowTest(TestCase):

    def setUp(self):
        pass

    def test_exec_flow(self):
        sys0 = Mock(spec=SystemCtl)
        sys0.is_dcqcn_enabled.return_value = True
        sys0.hostname = "sys0"

        sys1 = Mock(spec=SystemCtl)
        sys1.is_dcqcn_enabled.return_value = False
        sys1.get_version.return_value = ("", "20240910-xxx-yyy")
        sys1.hostname = "sys1"

        sys2 = Mock(spec=SystemCtl)
        sys2.is_dcqcn_enabled.return_value = False
        sys2.get_version.return_value = ("", "20241010-xxx-yyy")
        sys2.hostname = "sys2"

        ctx = FlowContext("mock")
        ok = SetSystemDcqcnFlow.exec(ctx, should_enable=True, sys_controllers=[sys0, sys1, sys2],
                                     noconfirm=True, skip_tasks=["ConfigureSystems"])
        self.assertTrue(ok)

        self.assertEquals(ctx.device_state["sys0"].status, FlowStatus.Success)
        self.assertEquals(ctx.device_state["sys0"].step, "Preflight")

        self.assertEquals(ctx.device_state["sys1"].status, FlowStatus.Fail)
        self.assertEquals(ctx.device_state["sys1"].step, "Preflight")

        self.assertEquals(ctx.device_state["sys2"].status, FlowStatus.Success)
        self.assertEquals(ctx.device_state["sys2"].step, "Preflight")
