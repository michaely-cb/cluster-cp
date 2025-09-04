# csadm

This directory includes scripts for cluster admins to manage a Cerebras cluster.
Note: these scripts are designed to run on the management node only.

The `csadm.sh` is the entry point script. It supports many commands in this form:

```
csadm.sh <command> <command_options>
```

`csadm.sh <command> [--help|-h]` shows detailed usage of the command.

Top-level Commands are listed as follows.

```
  csadm.sh install MANIFEST_FILE [--update-config[=CONFIG_FILE]] [--preflight] [--skip-k8s] [--validate] [--debug] [--yes]
    Installs cluster components described in the given manifest json file.

  csadm.sh update-cluster-config [NETWORK_CONFIG_FILE] [--create-depop-groups=N] [--dry-run] [--safe-system-update]
    Converts the given network config file to cluster.yaml format that can be
    used by cluster management.

  csadm.sh get-cluster [--system-only|--node-only] [--error-only]
    Get cluster state information including both systems and nodes

  csadm.sh update-node --name=<node_name> [--nic=<nic_name>] --state=<ok/error> --note="<reasoning>"
    Update node health status. This is mainly used to exclude an unhealthy node/NIC from being scheduled.
    Warning: after marked as error, the node/NIC will not be assigned for jobs until marked as ok.

  csadm.sh batch-update-nodes <file_path> <ok/error> \"<note>\"
    Batch update node health status with node line by line in the provided file.
    For updating to error, it will skip update if already in error to avoid overriding the note.
    For updating to ok, it will skip update if it has a different note than given one to avoid overriding.

  csadm.sh update-nodegroup --name=<nodegroup_name> --state=<ok/error> --note="<reasoning>"
    Update health status for all nodes in the specified nodegroup. This is mainly used to exclude an 
    unhealthy nodegroup from being scheduled.
    Warning: after marked as error, all nodes in the nodegroup will not be assigned for jobs until marked as 
    ok.

  csadm.sh update-system --name=<system_name> [--port=<port_name>] --state=<ok/error> --note="<reasoning>"
    Updates system health status. This is mainly used to exclude an error system/port from being scheduled
    Warning: after marked as error, the system/port will not be assigned for jobs until marked as ok.

  csadm.sh create-namespace --name=<namespace> [--systems=<system0,system1,...>]
    Create a namespace with optional system assignments. This is used for multibox sharing.

  csadm.sh delete-namespace --name=<namespace>
    Delete a namespace and release its assigned systems.

  csadm.sh update-namespace --name=<namespace> --systems=<system0,system1,...> [--mode=<append/append-all/remove/remove-all>]
    Update a namespace's system assignments.

  csadm.sh get-namespace [--name=<namespace>]
    Get namespaces and their system assignments.

  csadm.sh test-system-fabric-diag [--dry-run] [ all | SYSTEM... ]
    Runs fabric diag test against given systems after checking the systems are not in use and taken out of
    scheduling.

  csadm.sh reset-storage [--help] [--job-logs] [--cached-compile] [--user-venvs]
        [--cluster-volumes] [--custom-worker-images] [--worker-cache] [--all]
        [--monitoring-data] [--job-history] [--dry-run] [--use-manifest FILE]
    Resets application storage, clearing any potentially sensitive data.

  csadm.sh rotate-user-auth-secret
    Replace the existing user authentication secret with a new one. The cluster admin
    also needs to copy this new secret to all user nodes to replace the old one.
    
  csadm.sh assert-idle-systems [ all | SYSTEM... ]
    Asserts that the given systems are idle and can be taken out of scheduling.
    
  csadm.sh operate-systems [ all | SYSTEM... ] [ args ]
    Operate on the given systems. The args are passed to the system operation script.
    More details in the operate-systems section below.
    
  csadm.sh smart-uscd-update -d <absolute image directory> --system-cred <user>:<password> [ all | SYSTEM... ]
    Perform consolidated USCD update commands. This command will take care of USCD image
    upload, precheck, and update on target systems.
    
    More details in the smart-uscd-update section below.
```

## operate-systems

`csadm.sh operate-systems` under the hood calls the csadm_operate_systems.py script
to perform operations on a batch of systems. Here are the currently supported features:

```
  csadm.sh operate-systems [ all | SYSTEM... ] user:pwd system_show
    Show system status for the given systems.
    
  csadm.sh operate-systems [ all | SYSTEM... ] user:pwd software_image_show
    Lists the available WSE data images on systems.
    
  csadm.sh operate-systems [ all | SYSTEM... ] user:pwd system_standby
    Puts the given systems into standby mode.
    
  csadm.sh operate-systems [ all | SYSTEM... ] user:pwd system_activate
    Activates the given systems.
    
  csadm.sh operate-systems [ all | SYSTEM... ] user:pwd check_for_event <EventFilter> <systems_time>
    Checks for events on the given systems.
    An example:
    csadm.sh operate-systems system1,system2,system3 user:pwd \
    check_for_event EventStandby* 2021-09-01T00:00:00Z,2021-09-01T00:00:00Z,2021-09-01T00:00:00Z
    
    The example above will check for termination events for standby on 3 systems.
    Since each system will have its own clock, we need to pass in the time for
    when each of the systems were put into standby.
    
  csadm.sh operate-systems [ all | SYSTEM... ] user:pwd wse_config_show
    Shows the active and available WSE configuration for the given systems.
    
  csadm.sh operate-systems [ all | SYSTEM... ] user:pwd wse_config_set config1,config2...
    Sets the WSE configuration for the given systems.
    
  csadm.sh operate-systems [ all | SYSTEM... ] user:pwd wse_data_update image1,image2...
    Updates the WSE data images on the given systems. Must put system to standby
    first.

  csadm.sh select-upgrade-nodes <target-version> [session-name] [skip-sx-pick]
    Choose nodes for upgrade, skip nodes already matching target version.
    If session name provided, will pick up nodes within session + SX nodes based on systems assigned.
    If skip-sx-pick set to non-empty, SX will not be picked as part of session upgrade, useful for cases SX upgraded separately.
    If no session provided, will pick up nodes not in target version, useful for cases like unassigned/redundant nodes.

  csadm.sh cluster-operator-contacts <action>
    Manage the "cluster-operator-contacts" Secret in the "prometheus" namespace.
    Run 'csadm.sh cluster-operator-contacts --help' for more information.
    
```

## Smart USCD Update

`csadm.sh smart-uscd-update` commands will perform the consolidated steps for:
1. Upload USCD image to the target systems.
2. Precheck the target systems, ensure their state is as expected.
3. Update the USCD image on the target systems.

NOTE: As with other types of maintenance work, please ensure that the system has been
marked as in ERROR state first. 

The image upload and precheck should take less than 10 seconds. The update process
will take ~20 minutes. 

Here's an example command:

```bash
csadm.sh smart-uscd-update -d $PWD --system-cred admin:admin systemf34,xs10097
```

* `-d` is the absolute path to the directory containing the USCD image.
* `--system-cred` is the username and password for the systems.
* `systemf34,xs10097` are the systems to update. You can optionally use `all` to update
all systems.
