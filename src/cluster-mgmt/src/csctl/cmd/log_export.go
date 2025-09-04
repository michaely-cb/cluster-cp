package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csctlv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"cerebras.com/cluster/csctl/pkg"
)

type exportTargetType string

var localTargetType exportTargetType = "local"
var debugTargetType exportTargetType = "debug-volume"
var nfsTargetType exportTargetType = "nfs"

type LogExportCmdOptions struct {
	Name                    string
	Path                    string
	NumLines                uint64
	Binaries                bool
	IncludeCompileArtifacts bool
	TimeoutSeconds          uint64
	Tasks                   string
	Level                   int64
	Verbosity               int64

	// Starting from release 2.1, non-root user can no longer export cluster logs
	// for isolation considerations.
	// SkipClusterLogs is hidden and does not have any effect for non-root users.
	// The property is always true for non-root users and default to false for the root user.
	SkipClusterLogs bool

	// If logs are from NFS, log export only creates symlinks to the logs by default.
	// Only if this flag is raised, log export will copy the files from NFS.
	CopyNfsLogs bool

	// One of "local", "debug-volume", "nfs".
	// Default is "local", where the artifacts will be sent back to users' local disk.
	// For "debug-volume" and "nfs", artifacts will not be sent back to users.
	TargetType string

	LogExportJobId string

	Force bool

	cmdCtx *pkg.CmdCtx
	outErr OutErr
}

func NewLogExportCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := LogExportCmdOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use: "log-export NAME [-b] [-c] [-p <path>] [--target-type <target-type>] [--tasks <tasks>] " +
			"[--copy-nfs-logs] [--num-lines <numLines>] [--timeout <seconds>] [--force]",
		Short: "Gather and download logs.",
		Long:  `Gather and download logs and artifacts from the cluster for a given job that is owned by the user group.`,
		Example: `
csctl log-export wsjob-0001

csctl log-export wsjob-0001 --timeout 3600 --tasks activation,weight

csctl log-export wsjob-0001 --timeout 3600 --binaries --compile-artifacts --tasks coordinator-0,chief-0

csctl log-export wsjob-0001 -b -c -p ./jobs`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if options.TimeoutSeconds <= 0 {
				return fmt.Errorf("Timeout %d (in seconds) needs to be a positive integer.", options.TimeoutSeconds)
			}

			done := make(chan error, 1)
			go func() {
				if err := options.Complete(cmd, cmdCtx, args); err != nil {
					done <- err
					return
				}

				err := options.Run(cmdCtx.Context)
				done <- err
			}()
			select {
			case err := <-done:
				return err
			case <-time.After(time.Duration(options.TimeoutSeconds) * time.Second):
				return fmt.Errorf("Job timed out after %d seconds.", options.TimeoutSeconds)
			}
		},
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&options.Path, "path", "p", ".", "If gathering is complete, download log archive here.")
	cmd.Flags().Uint64VarP(&options.NumLines, "num-lines", "", 0, "If non-zero retrieve n last lines for each log. (default: 0)")
	cmd.Flags().BoolVarP(&options.Binaries, "binaries", "b", false, "Include binary debugging artifacts. (default: false)")
	cmd.Flags().BoolVarP(&options.IncludeCompileArtifacts, "compile-artifacts", "c", false,
		"Include all compile artifacts including elfs. (default: false)")
	cmd.Flags().Uint64VarP(&options.TimeoutSeconds, "timeout", "", 3600,
		"The maximum duration in seconds where log export could elapse.")
	cmd.Flags().StringVar(&options.Tasks, "tasks", "", "A comma-separated filter specifying the tasks "+
		"which should only be exported. If not specified, all tasks will get exported. Example: '--tasks coordinator-0,chief-0' exports "+
		"1 coordinator and 1 chief, whereas '--tasks chief' exports all chiefs . (default: \"\")")
	cmd.Flags().StringVarP(&options.TargetType, "target-type", "", "local", "One of \"local\", \"debug-volume\" and \"nfs\". "+
		"Artifacts will be sent back to users' local disk only when \"local\" target type is used. "+
		"When \"debug-volume\" target type is used, the artifacts will be stored in the appliance for debug purposes. "+
		"When \"nfs\" target type is used, the artifacts can be stored on an NFS directory. (default: \"local\") ")
	cmd.Flags().BoolVarP(&options.CopyNfsLogs, "copy-nfs-logs", "", false,
		"Copy logs from NFS instead of creating symlinks. (default: false)")
	cmd.Flags().BoolVarP(&options.SkipClusterLogs, "skip-cluster-logs", "", false,
		"Skip cluster management logs. Non-root users will always skip. (default: false)")
	cmd.Flags().BoolVarP(&options.Force, "force", "", false,
		"Force log export even from within the cluster (not recommended). (default: false)")
	cmd.Flags().Lookup("skip-cluster-logs").Hidden = true

	return SetDefaults(cmd)
}

func (c *LogExportCmdOptions) Complete(cmd *cobra.Command, cmdCtx *pkg.CmdCtx, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing required argument NAME")
	}
	c.Name = args[0]

	allowedTargetTypes := []string{string(localTargetType), string(debugTargetType), string(nfsTargetType)}
	if !slices.Contains(allowedTargetTypes, c.TargetType) {
		return fmt.Errorf("target type needs to be one of %v", allowedTargetTypes)
	}

	if c.cmdCtx.OnMgmtNode {
		err := validateMgmtNodeLogExport(c.cmdCtx.SingleNodeCluster, c.Force, cmd.OutOrStdout())
		if err != nil {
			return fmt.Errorf("failed to validate log export: %v", err)
		}
	}

	c.outErr = CmdOutErr{cmd: cmd}
	return nil
}

func (c *LogExportCmdOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	loopDelay := 10 * time.Second

	job := &csctlv1.Job{}
	jobFound := true
	getReq := &pb.GetRequest{
		Type:   "jobs",
		Name:   c.Name,
		Accept: pb.SerializationMethod_PROTOBUF_METHOD,
	}
	getRes, err := client.Get(ctx, getReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				jobFound = false
			}
		}
	} else {
		if err = proto.Unmarshal(getRes.GetRaw(), job); err != nil {
			return fmt.Errorf("failed to deserialize response from server: %v", err)
		}

		// Sanity check on input tasks and opportunistically reject early
		if c.Tasks != "" && job.Status != nil && len(job.Status.Replicas) != 0 {
			knownReplicaTypes := map[string]bool{}
			knownTasks := map[string]bool{}
			for _, replicaGroup := range job.Status.Replicas {
				knownReplicaTypes[replicaGroup.Type] = true
				for _, replicaInst := range replicaGroup.Instances {
					task := strings.TrimPrefix(replicaInst.Name, fmt.Sprintf("%s-", c.Name))
					knownTasks[task] = true
				}
			}

			tasks := strings.Split(c.Tasks, ",")
			var sortedTasks []string
			var invalidTasks []string
			for _, t := range tasks {
				tLowered := strings.ToLower(t)
				if knownReplicaTypes[tLowered] || knownTasks[tLowered] {
					sortedTasks = append(sortedTasks, tLowered)
				} else {
					invalidTasks = append(invalidTasks, tLowered)
				}
			}

			if len(invalidTasks) != 0 {
				return fmt.Errorf("found invalid task(s): %v. Please use `csctl -d2 get job` to check the tasks.", invalidTasks)
			}
			// Use sorted order to dedupe
			slices.Sort(sortedTasks)
			c.Tasks = strings.Join(sortedTasks, ",")
		}
	}

	skipClusterLogs := false
	if c.cmdCtx.Uid != 0 {
		skipClusterLogs = true
		if !jobFound {
			return fmt.Errorf(
				"job '%s/%s' is not found. %s",
				c.cmdCtx.Namespace, c.Name, SysAdminSupportMsg,
			)
		} else if err != nil {
			return pkg.FormatGrpcError(err)
		}
	} else {
		skipClusterLogs = c.SkipClusterLogs
	}

	targetType := pb.ExportTargetType_EXPORT_TARGET_TYPE_CLIENT_LOCAL
	outputPath := ""
	if c.TargetType == string(debugTargetType) {
		targetType = pb.ExportTargetType_EXPORT_TARGET_TYPE_DEBUG_VOLUME
	} else if c.TargetType == string(nfsTargetType) {
		targetType = pb.ExportTargetType_EXPORT_TARGET_TYPE_NFS
		// server only needs to know about the output path in NFS export mode
		outputPath = c.Path
	}

	req := &pb.CreateLogExportRequest{
		Name:                    c.Name,
		TimeoutSeconds:          int64(c.TimeoutSeconds),
		Binaries:                c.Binaries,
		IncludeCompileArtifacts: c.IncludeCompileArtifacts,
		CopyNfsLogs:             c.CopyNfsLogs,
		SkipClusterLogs:         skipClusterLogs,
		Level:                   c.Level,
		NumLines:                int64(c.NumLines),
		Tasks:                   c.Tasks,
		ExportTargetType:        targetType,
		OutputPath:              outputPath,
	}
	res, err := client.CreateLogExport(ctx, req)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}

	c.LogExportJobId = res.ExportId
	spin := pkg.NewSpinner(os.Stdout)
	spin.SetSuffix(" Gathering log data within cluster...")
	spin.Start()

	for {
		// We enter up an infinite loop for the case that the command is going to block,
		// waiting for the logs be available for download. This allows us to poll the server
		// with requests so we can track the progress of the job, and eventually to learn
		// that it's ready for download.
		req := &pb.GetLogExportRequest{
			ExportId: c.LogExportJobId,
		}
		res, err := client.GetLogExport(ctx, req)
		if err != nil {
			return pkg.FormatGrpcError(err)
		}

		switch res.Status {
		case pb.LogExportStatus_LOG_EXPORT_STATUS_UNSPECIFIED:
			spin.Stop()
			fmt.Printf("Cluster reported an error: unspecified : %s\n", c.LogExportJobId)
			return nil
		case pb.LogExportStatus_LOG_EXPORT_STATUS_FAILED:
			spin.Stop()
			fmt.Printf("Cluster reported an error: log-extract job failed: %s\n", c.LogExportJobId)
			fmt.Println(res.StatusDetail)
			return nil
		case pb.LogExportStatus_LOG_EXPORT_STATUS_RUNNING:
			fmt.Println(res.StatusDetail)
			// Delay and then loop back to message server and check status again...
			time.Sleep(loopDelay)
		case pb.LogExportStatus_LOG_EXPORT_STATUS_COMPLETED:
			spin.Done("Gathering log data within cluster...\n")

			// If user specified to use the debug volume or an nfs target path,
			// we will skip downloading the artifacts.
			if c.TargetType == string(localTargetType) {
				if err := downloadLogArchive(c, ctx); err != nil {
					return err
				}
			} else if c.TargetType == string(debugTargetType) {
				fmt.Println("Logs are exported to the debug volume")
			} else {
				fmt.Println("Logs are exported under " + path.Join(c.Path, c.LogExportJobId))
			}
			return nil
		}
	}
}

func downloadLogArchive(c *LogExportCmdOptions, ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	var recv struct {
		Offset             int64
		Signature          string
		NextPageToken      string
		TotalBytesExpected int64
	}

	var session struct {
		bytesAtStart       int64
		bytesReceived      int64
		timeStarted        time.Time
		timeUpdateNextFile time.Time
	}

	archiveName := path.Join(c.Path, fmt.Sprintf("%s.zip", c.LogExportJobId))
	//TODO[chad]: Confirm at this point that the archiveName .ZIP file doesn't exist, prompting
	//            user and skipping all of this action if they would like to investigate it.
	//TODO[chad]: If it DOES exist, we may want to send a dummy request to the server so it replies
	//            back with an archive signature we can use to notify the user if it seems their
	//            local archive version is stale...

	// Look for artifacts of a prior download attempt and determine if we have enough
	// information to attempt to resume downloading.
	freshStart := false
	recv.Offset = 0
	pf, err := os.Open(archiveName + ".part")
	if errors.Is(err, os.ErrNotExist) {
		// No partial download data. Normal for new requests, so we don't issue error message here.
		freshStart = true
	} else {
		nextData, err := os.ReadFile(archiveName + ".next")
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("Partial download exists, but next token key is missing.")
			freshStart = true
		} else {
			err = json.Unmarshal(nextData, &recv)
			if err != nil {
				fmt.Println("Unable to recover progress data from '.next' file.")
				freshStart = true
			}
		}
		partFileInfo, _ := pf.Stat()
		defer pf.Close()
		if recv.Offset == 0 {
			freshStart = true
		} else if partFileInfo.Size() < recv.Offset {
			fmt.Println("Partial archive and progress data do not match.")
			freshStart = true
		}
	}

	// Initialize state based on what we have learned
	if !freshStart {
		mbDone := float64(recv.Offset) / float64(1048576)
		pctComplete := 100.0
		if recv.TotalBytesExpected > 0 {
			pctComplete = 100.0 * (float64(recv.Offset) / float64(recv.TotalBytesExpected))
		}
		fmt.Printf("Partial download exists: %.2f MB. (%.2f%% of the archive)\n",
			mbDone, pctComplete)
		if false == pkg.ConfirmYNWithTimeout(true, 60, "Do you want to resume this download? (Y/n) : ") {
			freshStart = true
			pf = nil
		}
	}
	// Don't use an 'else', check the flag anew one final time, in case user elected not to resume a partial download:
	if freshStart {
		_ = os.RemoveAll(archiveName + ".part")
		_ = os.RemoveAll(archiveName + ".next")
		recv.Offset = 0
		recv.NextPageToken = ""
		recv.Signature = ""
		recv.TotalBytesExpected = 0
		// Create a blank file, so as we enter the loop, regardless of what logic came before,
		// we will at that point be guaranteed a file exists to open up for RDWR.
		pf, err = os.Create(archiveName + ".part")
		if err != nil {
			fmt.Printf("Error creating output file: %s (%v)\n", archiveName+".part", err)
			return err
		}
		session.bytesAtStart = 0
	} else {
		session.bytesAtStart = recv.Offset
	}
	session.bytesReceived = 0
	session.timeStarted = time.Now()
	session.timeUpdateNextFile = time.Now().Add(15 * time.Second)

	spin := pkg.NewSpinner(os.Stdout)
	spin.SetSuffix(" Downloading ... ")
	spin.Start()

	// Open the partial archive for applying new chunks of data
	pf, err = os.OpenFile(archiveName+".part", os.O_RDWR, 0644)
	if err != nil {
		spin.Stop()
		fmt.Printf("Error opening the partial archive to receive new chunks == %v\n", err)
		return err
	}
	defer pf.Close()
	// Seek to the end of the *last known complete chunk* written (may not be end of file)
	_, err = pf.Seek(recv.Offset, 0)
	if err != nil {
		spin.Stop()
		fmt.Println(c.outErr.Err(), "\nError seeking into the archive to insert the next chunk == %v\n", err)
		return err
	}

	for {
		// Request the next fragment of the archive:
		req := &pb.DownloadLogExportRequest{
			ExportId:  c.LogExportJobId,
			PageToken: recv.NextPageToken,
		}
		res, err := client.DownloadLogExport(ctx, req)
		if err != nil {
			spin.Stop()
			fmt.Printf("Cluster reports an error: LogExportGetDataResponse.err == %v\n", err)
			return pkg.FormatGrpcError(err)
		}

		if recv.Signature != "" {
			// Test for signature match, if we are aware of one.
			if recv.Signature != res.Signature {
				spin.Stop()
				fmt.Println("ERROR: Remote archive signature does not match partially downloaded file!\n" +
					"Please try again, choosing to NOT resume this partial download.")
				return err
			}
		} else {
			// This is the first message in a sequence.
			recv.Signature = res.Signature
			fmt.Println("Starting a fresh download of log archive.")
		}

		// Write the new chunk into the partial archive
		// NOTE: We will update the `.next` file afterwards, once we store this new chunk without incident.
		//       The `.part` data is always "journal'ed out" ahead of that cursor for safe resumption.
		chunkBytes, err := pf.Write(res.Chunk)
		if err != nil {
			spin.Stop()
			fmt.Printf("Error attempting to write to the partial archive == %v\n", err)
			return err
		}

		session.bytesReceived += int64(chunkBytes)
		mbDone := float64(session.bytesReceived+session.bytesAtStart) / float64(1048576)

		// Test to see if we are done receiving the archive
		if res.NextPageToken == "" {
			os.Rename(archiveName+".part", archiveName)
			os.RemoveAll(archiveName + ".next")
			spin.Done(fmt.Sprintf(" Downloaded %.2f MB.   ", mbDone))
			break // Leave the download loop!
		} else {
			// Alas, there is another page! Continue to download.
			recv.Offset += int64(chunkBytes)
			recv.NextPageToken = res.NextPageToken
			recv.TotalBytesExpected = session.bytesAtStart + session.bytesReceived + res.EstimatedRemainingBytes
			//
			pctComplete := 100.0
			if recv.TotalBytesExpected > 0 {
				pctComplete = 100.0 * (float64(session.bytesReceived+session.bytesAtStart) / float64(recv.TotalBytesExpected))
			}

			bps := float64(session.bytesReceived) / (time.Now().Sub(session.timeStarted)).Seconds()
			estMinToComplete := (float64(res.EstimatedRemainingBytes) / bps) / 60.0

			spin.SetSuffix(fmt.Sprintf(" Downloading: %.2f MB: %.2f%%   (Estimated time left: %.1f minutes) ",
				mbDone, pctComplete, estMinToComplete))

			// Obliterate the `.next` file and recreate it with updated values.
			// Overwriting it is fine because we don't read from it during a download.
			if session.timeUpdateNextFile.Before(time.Now()) {
				session.timeUpdateNextFile = time.Now().Add(15 * time.Second)
				nextFileJson, _ := json.MarshalIndent(recv, "", " ")
				nf, err := os.Create(archiveName + ".next") // Write out a new progress-tracking file.
				if err != nil {
					spin.Stop()
					fmt.Printf("Error creating progress tracking file: %s (%v)\n", archiveName+".next", err)
					return err
				}
				nf.Write(nextFileJson)
				nf.Close()
			}
		}
	}
	pf.Sync()
	fmt.Printf("Logs archive: %s\n", archiveName)
	return nil
}

// validateMgmtNodeLogExport returns an error if log-export is disallowed.
// It also writes any needed warnings to out.
func validateMgmtNodeLogExport(singleNodeCluster, force bool, out io.Writer) error {

	if singleNodeCluster {
		fmt.Fprintln(out,
			"Warning: Allowing log-export without --force since cluster is single-node. This may cause disk pressure.",
		)
		return nil
	}

	if !force {
		return fmt.Errorf(
			"log export is not allowed from within the cluster unless --force is specified. " +
				"Running log export on the management node may cause disk pressure. " +
				"Please run this command from a user node or specify --force if you understand the risks",
		)
	}

	fmt.Fprintln(out,
		"Warning: Exporting logs inside the cluster is not recommended and may cause disk pressure.",
	)
	return nil

}
