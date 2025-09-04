package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kr/text"
	"github.com/spf13/cobra"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/csctl/pkg"
)

var acceptedOutFmt = map[string]interface{}{
	"json": "",
	"yaml": "",
}

type LabelCmdOptions struct {
	Type string
	Name string

	Labels       map[string]string
	RemoveLabels []string

	Output     string
	DebugLevel int

	cmdCtx *pkg.CmdCtx
	outErr OutErr
}

func NewLabelCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := LabelCmdOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "label TYPE NAME [key=value...] [-o json|yaml]",
		Short: "Label resources",
		Long:  `Set or remove label key/value pairs on a resource.`,
		Example: text.Indent(`# set label "foo" to "val" and remove label with key "bar"
csctl label job wsjob-001 foo=val bar-

# set label "foo" to "bar" and get the updated object back in JSON
csctl label job wsjob-001 foo=bar -ojson`, "  "),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, cmdCtx, args); err != nil {
				return err
			}

			return options.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVarP(&options.Output, "output", "o", "",
		fmt.Sprintf("Output format. Empty defaults to a success message. Also supports %v", pkg.Keys(acceptedOutFmt)))
	return SetDefaults(cmd)
}

func (c *LabelCmdOptions) Complete(cmd *cobra.Command, cmdCtx *pkg.CmdCtx, args []string) error {
	if _, ok := acceptedOutFmt[c.Output]; !ok && c.Output != "" {
		return fmt.Errorf("unhandled output format, accepted: %v", pkg.Keys(acceptedOutFmt))
	}
	if len(args) < 3 {
		return fmt.Errorf("missing required arguments: TYPE NAME [labels...]")
	}
	c.Type = args[0]
	c.Name = args[1]
	c.DebugLevel = cmdCtx.DebugLevel
	labelMap, removeLabels, err := parseLabels(args[2:])
	if err != nil {
		return err
	}
	c.Labels = labelMap
	c.RemoveLabels = removeLabels

	c.outErr = CmdOutErr{cmd: cmd}
	return nil
}

func parseLabels(labelsUnparsed []string) (map[string]string, []string, error) {
	labels := map[string]string{}
	removeLabels := []string{}
	for _, arg := range labelsUnparsed {
		i := strings.Index(arg, "=")
		if i > -1 {
			if strings.LastIndex(arg, "=") != i {
				return nil, nil, fmt.Errorf(
					"bad argument: %s should be in format key=value "+
						"where key and value must not contain '='", arg)
			}
			labels[arg[0:i]] = arg[i+1:]
		} else {
			if strings.HasSuffix(arg, "-") {
				removeLabels = append(removeLabels, arg[0:len(arg)-1])
			} else {
				labels[arg] = ""
			}
		}
	}
	return labels, removeLabels, nil
}

func (c *LabelCmdOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	labelPatch := map[string]*string{}
	for k, v := range c.Labels {
		labelPatch[k] = pkg.Ptr(v)
	}
	for _, k := range c.RemoveLabels {
		labelPatch[k] = nil
	}
	body, err := json.Marshal(map[string]map[string]interface{}{"meta": {"labels": labelPatch}})
	if err != nil {
		return err
	}
	res, err := client.Patch(
		ctx,
		&pb.PatchRequest{
			Type:        c.Type,
			Name:        c.Name,
			PatchType:   pb.PatchRequest_MERGE,
			ContentType: pb.SerializationMethod_JSON_METHOD,
			Accept:      pb.SerializationMethod_JSON_METHOD,
			Body:        body,
		},
	)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}
	if c.Output != "" {
		return outputHandler[c.Output]([]GenericMessage{res}, c.outErr, c.DebugLevel)
	} else {
		fmt.Printf("%s/%s was patched\n", c.Type, c.Name)
	}
	return nil
}
