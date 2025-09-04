package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/kr/text"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csctlv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"cerebras.com/cluster/csctl/pkg"
)

type CheckVolumesCmdOptions struct {
	// Output format. One of table, json. Emtpy defaults to table
	Output string
	cmdCtx *pkg.CmdCtx
	outErr OutErr
}

type CheckVolumeResult struct {
	Name          string `json:"name"`
	ServerPath    string `json:"serverPath"`
	ContainerPath string `json:"containerPath"`
	PathExists    bool   `json:"pathExists"`
	ValidMountDir bool   `json:"validMountDir"`
	AllowVenv     bool   `json:"allowVenv"`
	WorkdirLogs   bool   `json:"workdirLogs"`
	CachedCompile bool   `json:"cachedCompile"`
	Details       string `json:"details"`
}

func NewCheckVolumesCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := CheckVolumesCmdOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "check-volumes",
		Short: "Check volume validity on this usernode",
		Long: `Check validity of NFS volumes on the usernode.

This command helps determine if a volume can be passed to the --mount_dirs option for ML jobs.
Columns/Keys:
name:          Name of the volume
serverPath:    NFS server path for this volume
containerPath: Mount path for this volume inside a container
pathExists:    Boolean to indicate if the containerPath of the volume exists on this usernode (JSON-only)
validMountDir: Boolean to indicate if the containerPath of the volume can be passed to --mount_dirs option for ML jobs
allowVenv:     Boolean to indicate if this is a volume in which Python virtual environment can be replicated to get dependency parity
details:       Description of validity check results`,
		Example: text.Indent(`# List volume validity checks in a table
csctl check-volumes

# Show volume validity check output in JSON format
csctl check-volumes -ojson`, "  "),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, cmdCtx, args); err != nil {
				return err
			}

			return options.Run(cmdCtx.Context)
		},
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&options.Output, "output", "o", "table", "Output format. One of table or json")
	return SetDefaults(cmd)
}

func checkVolumes(vols *csctlv1.VolumeList) (bool, []CheckVolumeResult) {
	allowVenv := false
	var results []CheckVolumeResult

	var cvr CheckVolumeResult

	var isNfs bool
	var sameDir bool
	var sameServer bool

	// for each volume
	for _, v := range vols.Items {
		cvr = CheckVolumeResult{}
		cvr.Name = v.Meta.Name
		// check allow_venv status
		if nfsProps := v.GetNfs(); nfsProps != nil {
			if av := v.Meta.Labels["allow-venv"]; av == strconv.FormatBool(true) {
				cvr.AllowVenv = true
				allowVenv = true
			}
			if av := v.Meta.Labels["workdir-logs"]; av == strconv.FormatBool(true) {
				cvr.WorkdirLogs = true
			}
			if av := v.Meta.Labels["cached-compile"]; av == strconv.FormatBool(true) {
				cvr.CachedCompile = true
			}
			cvr.ContainerPath = nfsProps.ContainerPath
			srvIp := nfsProps.Server
			srvPath := nfsProps.ServerPath
			cvr.ServerPath = fmt.Sprintf("%s:%s", srvIp, srvPath)
			if pathExists := pkg.PathExists(cvr.ContainerPath); pathExists {
				// Is that path mounted as NFS and from the same IP and
				// server path?
				cvr.PathExists = true
				isNfs, sameDir, sameServer = pkg.CheckNfsMount(srvIp, srvPath, cvr.ContainerPath)
				if isNfs {
					if sameServer && sameDir {
						if cvr.AllowVenv {
							cvr.Details += fmt.Sprintf("Appliance workflow can automatically copy " +
								"Python virtual environments to this volume to get dependency parity in case of image build job failures.")
						} else if cvr.WorkdirLogs {
							cvr.Details += fmt.Sprintf("Appliance workflow can be configured to write workdir logs to this volume.")
						} else if cvr.CachedCompile {
							cvr.Details += fmt.Sprintf("Appliance workflow can be configured to write cached compile artifacts to this volume.")
						} else {
							cvr.Details += fmt.Sprintf("Volume and its sub-directories can be passed to ML jobs via '--mount_dirs %s'. ", cvr.ContainerPath)
						}
						cvr.ValidMountDir = true
					} else {
						if !sameServer {
							cvr.Details += "Volume is mounted from a different server on the usernode. "
						}
						if !sameDir {
							cvr.Details += "Volume is mounted from a different server path on the usernode. "
						}
					}
				} else {
					cvr.Details += fmt.Sprintf("Volume is not NFS mounted from %s. ", cvr.ServerPath)
				}
			} else {
				cvr.Details += fmt.Sprintf("%s does not exist on usernode. ", cvr.ContainerPath)
			}

			results = append(results, cvr)
		}
	}

	return allowVenv, results
}

func ShowAsTable(results []CheckVolumeResult, opts *CheckVolumesCmdOptions) error {
	table := csctlv1.Table{Columns: []*csctlv1.ColumnDefinition{
		{Name: "name"},
		{Name: "server_path"},
		{Name: "container_path"},
		{Name: "allow_venv"},
		{Name: "valid_mount_dir"},
		{Name: "details"},
	}}
	var cells []string
	for _, r := range results {
		cells = []string{
			r.Name,
			r.ServerPath,
			r.ContainerPath,
			strconv.FormatBool(r.AllowVenv),
			strconv.FormatBool(r.ValidMountDir),
			r.Details,
		}
		table.Rows = append(table.Rows, &csctlv1.RowData{Cells: cells})
	}

	return DisplayTable(&table, nil, opts.outErr)
}

func (c *CheckVolumesCmdOptions) Complete(cmd *cobra.Command, cmdCtx *pkg.CmdCtx, args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("the check-volumes command does not take more than one argument")
	}

	if c.Output != "" && c.Output != "table" && c.Output != "json" {
		return fmt.Errorf("invalid output format. Valid options are 'table' or 'json'")
	}

	c.outErr = CmdOutErr{cmd: cmd}
	c.cmdCtx = cmdCtx
	return nil
}

func (c *CheckVolumesCmdOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	req := &pb.GetRequest{
		Type:   "volumes",
		Accept: pb.SerializationMethod_PROTOBUF_METHOD,
	}

	res, err := client.Get(ctx, req)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}

	vols := &csctlv1.VolumeList{}
	if err = proto.Unmarshal(res.GetRaw(), vols); err != nil {
		return fmt.Errorf("failed to deserialize response from server: %v", err)
	}
	allowVenv, results := checkVolumes(vols)

	if c.Output == "" || c.Output == "table" {
		err = ShowAsTable(results, c)
		if err != nil {
			return err
		}
	} else {
		b, err := json.MarshalIndent(&results, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to serialize to JSON: %v", err)
		}
		_, err = fmt.Fprintln(c.outErr.Out(), string(b))
		if err != nil {
			return err
		}
	}

	if !allowVenv {
		return fmt.Errorf("no valid volumes with the 'allow-venv' flag set. " +
			"The cluster sysadmin must configure one using cluster-volumes.sh on the management node")
	}
	return nil
}
