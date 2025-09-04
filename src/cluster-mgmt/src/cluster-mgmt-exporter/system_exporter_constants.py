prom_probe_timeout = 60

# https://github.com/Cerebras/platform/blob/master/idl/cli/system/status_cs2.proto
# some status maybe deprecated
status_mapping = {
    "SYSTEM_NOT_REACHABLE": -2,
    "ADMIN_SVC_UNKNOWN": -1,
    "UNKNOWN": 0,
    "OK": 1,
    "DEGRADED": 2,
    "CRITICAL": 3,
    "CRITICAL_EMERGENCY_SHUTDOWN": 4,
    "STANDBY": 5,
    "PENDING_STANDBY": 6,
    "PENDING_ACTIVE": 7,
    "MISSING": 8,
    "NO_POWER": 9,
    "HARDWARE_ERROR": 10,
    "DISABLED": 11,
    "UNAVAILABLE": 12,
    "UPDATING_SW": 13,
    "PUMP_BLEEDING": 14,
    "SERVICES_NOT_READY": 15,
    "FPGA_UPDATE": 16,
    "PUMP_LIMPING": 17,
    "DOOR_OPEN": 18,
    "DOOR_CLOSED": 19,
    "NON_STANDARD": 20,
}


def resolve_event_to_status(events):
    status = ""
    for event in events:
        event_id = event.get("type")
        mapping = eventMapping.get(event_id, {"statusMapping": ""})
        cur_status = mapping.get("statusMapping", "")
        status = max_status(cur_status, status)
    return status


def max_status(a, b):
    # ranking of statusMapping values based on severity. Higher severity has higher index
    status_rank = [
        "",
        "UNKNOWN",
        "OK",
        "PENDING_STANDBY",
        "PENDING_ACTIVE",
        "STANDBY",
        "UPDATING_SW",
        "PUMP_BLEEDING",
        "SERVICES_NOT_READY",
        "FPGA_UPDATE",
        "PUMP_LIMPING",
        "DOOR_OPEN",
        "DOOR_CLOSED",
        "DEGRADED",
        "MISSING",
        "NO_POWER",
        "CRITICAL",
        "CIRITICAL_EMERGENCY_SHUTDOWN",
        "DISABLED",
        "UNAVAILABLE",
        "HARDWARE_ERROR"
    ]

    a_idx = status_rank.index(a)
    b_idx = status_rank.index(b)

    # return status value that is more severe
    return a if a_idx > b_idx else b


event_mapping = {
    "VGEventWithPrimitiveFields": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventSecondEventInFile": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventCMMgrStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventWaferServicesFailed": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventWaferServicesStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventWaferReady": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventWaferServicesStopped": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventTrainingStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventTrainingCompleted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventTrainingAborted": {
        "severity": "WARN",
        "statusMapping": ""
    },
    "EventConfigHang": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFirstControlConnectionOpen": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventLastControlConnectionClosed": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventFirstSquadronReady": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventLastSquadronDone": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventProgrammingStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventProgrammingFinished": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventFabricDumpStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventFabricDumpFinished": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventFanHardwareError": {
        "severity": "CRITICAL",
        "statusMapping": "HARDWARE_ERROR"
    },
    "EventPsuHardwareError": {
        "severity": "CRITICAL",
        "statusMapping": "HARDWARE_ERROR"
    },
    "EventPsuNoPower": {
        "severity": "CRITICAL",
        "statusMapping": "NO_POWER"
    },
    "EventFanMissing": {
        "severity": "CRITICAL",
        "statusMapping": "MISSING"
    },
    "EventPsuMissing": {
        "severity": "CRITICAL",
        "statusMapping": "MISSING"
    },
    "EventPumpMissing": {
        "severity": "CRITICAL",
        "statusMapping": "MISSING"
    },
    "EventPumpHardareError": {
        "severity": "CRITICAL",
        "statusMapping": "HARDWARE_ERROR"
    },
    "EventFailedToCollectSupportBundle": {
        "severity": "WARN",
        "statusMapping": ""
    },
    "EventTimezoneUpdated": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventUpgradeStarted": {
        "severity": "INFO",
        "statusMapping": "UPDATING_SW"
    },
    "EventUpgradeCompleted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventUpgradeFailedWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventUpgradeFailedCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventUpgradeStateUpdateFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventUpgradeStateInvalid": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventPendingActive": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventPendingIoService": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventPendingActiveInternal": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventEnteredActive": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventNoWaferStartActivate": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventEnteredActiveInternal": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventPendingStandby": {
        "severity": "INFO",
        "statusMapping": "STANDBY"
    },
    "EventPendingStandbyInternal": {
        "severity": "INFO",
        "statusMapping": "STANDBY"
    },
    "EventEnteredStandby": {
        "severity": "INFO",
        "statusMapping": "STANDBY"
    },
    "EventEnteredStandbyInternal": {
        "severity": "INFO",
        "statusMapping": "STANDBY"
    },
    "EventIoHardwareError": {
        "severity": "CRITICAL",
        "statusMapping": "HARDWARE_ERROR"
    },
    "EventAclHardwareError": {
        "severity": "CRITICAL",
        "statusMapping": "HARDWARE_ERROR"
    },
    "EventHealthDegraded": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventHealthCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventHealthOk": {
        "severity": "INFO",
        "statusMapping": "OK"
    },
    "EventHealthOkInternal": {
        "severity": "INFO",
        "statusMapping": "OK"
    },
    "EventHealthUnknown": {
        "severity": "WARN",
        "statusMapping": "UNKNOWN"
    },
    "EventManagementHardwareError": {
        "severity": "CRITICAL",
        "statusMapping": "HARDWARE_ERROR"
    },
    "EventClearConfigOpFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventConfigOpTimedout": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventConfigOpIssued": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventConfigOpCompleted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventConfigOpFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventActivateTimedout": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventActivateIssued": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventActivateCompleted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventActivateFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventStandbyTimedout": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventStandbyIssued": {
        "severity": "INFO",
        "statusMapping": "STANDBY"
    },
    "EventStandbyCompleted": {
        "severity": "INFO",
        "statusMapping": "STANDBY"
    },
    "EventStandbyFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventRebootFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventMModeExitFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventMModeEnterFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventDataCorruptionCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorTemperatureHighWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorTemperatureLowWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorTemperatureHighCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorTemperatureLowCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorVoltageHighWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorVoltageLowWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorVoltageHighCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorVoltageLowCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorCurrentHighWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorCurrentLowWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorCurrentHighCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorCurrentLowCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorFanSpeedHighWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorFanSpeedLowWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorFanSpeedHighCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorFanSpeedLowCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorFlowRateHighWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorFlowRateLowWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorFlowRateHighCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorFlowRateLowCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorFlowStabilityWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorFlowStabilityCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorPowerHighWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorPowerLowWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorPowerHighCritical": {
        "severity": "WARN",
        "statusMapping": "CRITICAL"
    },
    "EventSensorPowerLowCritical": {
        "severity": "WARN",
        "statusMapping": "CRITICAL"
    },
    "EventSensorHumidityHighWarn": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorHumidityLowWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventSensorHumidityHighCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSensorHumidityLowCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventRadiusServerAdded": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventRadiusServerRemoved": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventSSHKeyAdded": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventSSHKeyRemoved": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventSetWaferServiceMode": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventSetExecModePipelined": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventSetExecModeWeightStreaming": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventSetExecModeFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventWSEDataUpdateStarted": {
        "severity": "INFO",
        "statusMapping": "UPDATING_SW"
    },
    "EventWSEDataUpdateFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventWSEDataUpdateSuccess": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventBootLoaderUpgradeStarted": {
        "severity": "INFO",
        "statusMapping": "UPDATING_SW"
    },
    "EventBootloaderUpgradeCompleted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventBootLoaderUpgradeFailedCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventBootLoaderComponentUpgradeStarted": {
        "severity": "INFO",
        "statusMapping": "UPDATING_SW"
    },
    "EventBootloaderComponentUpgradeCompleted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventBootLoaderComponentUpgradeFailed": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventRepairTestStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventRepairTestPassed": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventRepairTestFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventRepairLoadStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventRepairLoadSucceeded": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventRepairLoadFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventRepairRetrieveStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventRepairRetrieveSucceeded": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventRepairRetrieveFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventDiagnosticsTestStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventDiagnosticsTestPassed": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventDiagnosticsTestFailed": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventDiagnosticsDataMissing": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventDiagnosticsTestSkipped": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventEmergencyShutdownWseCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL_EMERGENCY_SHUTDOWN"
    },
    "EventEmergencyShutdownWseInternalError": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL_EMERGENCY_SHUTDOWN"
    },
    "EventEmergencyShutdownCmInternalError": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL_EMERGENCY_SHUTDOWN"
    },
    "EventEmergencyShutdownSensorCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL_EMERGENCY_SHUTDOWN"
    },
    "EventComponentFailureSensorNotResponding": {
        "severity": "ERROR",
        "statusMapping": "CRITICAL"
    },
    "EventSuspendFanDewPointCalculation": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventDewPointDegradedCalculation": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventEmergencyShutdownDewPointCondition": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventEmergencyShutdownFanCountCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventEmergencyShutdownPumpCountCritical": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventEmergencyShutdownLeakAcl": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventEmergencyShutdownLeakPump": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventCriticalPumpLeakCableMissing": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventAssertMonitorBusStuck": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem6_0": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem6_1": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem6_2": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem7_0": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem7_1": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem7_2": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem8_0": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem8_1": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem8_2": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem9_0": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem9_1": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem9_2": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAUncorrIntEccMem10": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp0WrErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp0WrErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp0RdErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp0RdErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp1WrErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp1WrErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp1RdErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp1RdErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp2WrErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp2WrErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp2RdErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcTcp2RdErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds0WrErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds0WrErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds0RdErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds0RdErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds1WrErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds1WrErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds1RdErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds1RdErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds2WrErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds2WrErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds2RdErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcLvds2RdErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcPcieWrErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcPcieWrErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcPcieRdErr": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventFPGAHmcPcieRdErrDinv": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventHardwareMonitorStarted": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventHardwareMonitorShutdownInternalError": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL_EMERGENCY_SHUTDOWN"
    },
    "EventPecError": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventFirmwareMismatch": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventFPGAUpgradeStart": {
        "severity": "INFO",
        "statusMapping": "FPGA_UPDATE"
    },
    "EventFPGAUpgradeSuccess": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventFPGAUpgradeFailure": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventPMBusWarn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventPumpBleedComplete": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventPumpBleedTimedOut": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventFruPowerFailure": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventWaferVddPowerFailure": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSyskillTimeStamp": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventUnsupportedSystemConfig": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventSequencerFaultTimeStamp": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventAmbientTemperatureExceedLimit": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventCurrentExceedThresholdPowerOn": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventFabricReconfigured": {
        "severity": "INFO",
        "statusMapping": ""
    },
    "EventACLTempsFault": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    },
    "EventACLTempsAnon": {
        "severity": "WARN",
        "statusMapping": "DEGRADED"
    },
    "EventACLTemps": {
        "severity": "WARN"
    },
    "EventUsbHotplugAssert": {
        "severity": "CRITICAL",
        "statusMapping": "CRITICAL"
    }
}

component_mapping = {
    1: "DATA_NETWORK_IF_1",
    2: "DATA_NETWORK_IF_2",
    3: "DATA_NETWORK_IF_3",
    4: "DATA_NETWORK_IF_4",
    5: "DATA_NETWORK_IF_5",
    6: "DATA_NETWORK_IF_6",
    7: "DATA_NETWORK_IF_7",
    8: "DATA_NETWORK_IF_8",
    9: "DATA_NETWORK_IF_9",
    10: "DATA_NETWORK_IF_10",
    11: "DATA_NETWORK_IF_11",
    12: "DATA_NETWORK_IF_12",
    13: "DATA_CTRL_IF",
    14: "ACL",
    15: "SM",
    16: "CM",
    17: "IOM0",
    18: "FPGA0",
    19: "FPGA1",
    20: "IOM1",
    21: "FPGA2",
    22: "FPGA3",
    23: "IO",
    24: "IOVRM0",
    25: "IOVRM1",
    26: "MEM0",
    27: "MEM1",
    28: "FAN0",
    29: "FAN1",
    30: "FAN2",
    31: "FAN3",
    32: "FAN",
    33: "PSU",
    34: "PSU0",
    35: "PSU1",
    36: "PSU2",
    37: "PSU3",
    38: "PSU4",
    39: "PSU5",
    40: "PSU6",
    41: "PSU7",
    42: "PSU8",
    43: "PSU9",
    44: "PSU10",
    45: "PSU11",
    46: "PUMP0",
    47: "PUMP1",
    48: "VRM0",
    49: "VRM1",
    50: "VRM2",
    51: "VRM3",
    52: "VRM4",
    53: "VRM5",
    54: "VRM6",
    55: "VRM7",
    56: "VRM8",
    57: "VRM9",
    58: "VRM10",
    59: "VRM11",
    60: "LED",
    61: "PUMP",
    62: "CM_CM_DAEMON",
    63: "CM_WATCHTOWER",
    64: "CM_TCPMANAGER_1",
    65: "CM_TCPMANAGER_2",
    66: "CM_TCPMANAGER_3",
    67: "CM_TCPMANAGER_4",
    68: "CM_TCPMANAGER_5",
    69: "CM_TCPMANAGER_6",
    70: "CM_TCPMANAGER_7",
    71: "CM_TCPMANAGER_8",
    72: "CM_TCPMANAGER_9",
    73: "CM_TCPMANAGER_10",
    74: "CM_TCPMANAGER_11",
    75: "CM_TCPMANAGER_12",
    76: "THERMAL",
    77: "SYSTEM",
    78: "UPGRADESVC",
    79: "CM_CM_MANAGER",
    80: "CM_WAFER_SERVICES",
    81: "EXT_COOLANT_FLOW",
    82: "SM_HW_DAEMON",
    83: "CLIENT_COORDINATOR",
    84: "CLIENT_STREAMER",
    85: "CLIENT_TASK",
    86: "VRM",
    87: "STREAM_WEIGHT_DRIVER",
    88: "STREAM_CMD_DRIVER",
    89: "STREAM_STIMULANT",
    90: "STREAM_COLLECTIVE",
    91: "HLCFG",
    92: "WAFER_CONTROLLER",
    93: "WSE_HW"
}
