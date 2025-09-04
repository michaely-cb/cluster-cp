package pkg

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/status"
)

var internalTypeToExternal = map[string]string{
	"namespacereservations": "Session",
	"resourcelocks":         "Lock",
	"sysetms":               "System",
	"wsjobs":                "Wsjob",
}

var CerebrasVersion string // should be set by "go build"
var SemanticVersion string // should be set by "go build"

func Keys[k comparable, v any](m map[k]v) []k {
	var keys []k
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func Ptr[v any](val v) *v {
	return &val
}

func ReplaceHome(fp string) string {
	if strings.Index(fp, "$HOME") == 0 {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return fp
		}
		return homeDir + fp[5:]
	}
	return fp
}

// When gRPC errors occur, instead of returning the errors directly to end users,
// we attempt to return the error message part only, and map internal resource names to external facing ones.
// Original gRPC error example: "Error: rpc error: code = NotFound desc = namespacereservations.jobs.cerebras.com "xxx" not found"
// Massaged user-facing error example: "Error: Session "xxx" not found"
func FormatGrpcError(err error) error {
	s, ok := status.FromError(err)
	if !ok || len(s.Message()) == 0 {
		return err
	}

	reg := regexp.MustCompile(`([a-z]+)\.jobs\.cerebras\.com`)
	message := reg.ReplaceAllStringFunc(s.Message(), func(match string) string {
		prefix := reg.FindStringSubmatch(match)[1]

		if externalName, ok := internalTypeToExternal[prefix]; ok {
			return externalName
		}
		return ""
	})
	return errors.New(strings.TrimLeft(message, " "))
}

// TODO: Deprecate this if we adopted global cluster server in the future
// In release 2.1, we always talk the namespace-specific ingress with a namespace-specific authority
func buildNsAwareAuthority(baseAuthority string, namespace string) (string, error) {
	clusterServerSubdomain := "cluster-server"
	tokens := strings.Split(baseAuthority, ".")
	if tokens[0] != clusterServerSubdomain {
		return "", fmt.Errorf("unable to parse authority %s in csctl config", baseAuthority)
	} else {
		return fmt.Sprintf("%s.%s", namespace, baseAuthority), nil
	}
}

func PathExists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			fmt.Println("Path", path, "not accessible:", err)
		}
		return false
	} else {
		return true
	}
}

func CheckNfsMount(server string, serverPath string, mountPath string) (isNfs bool, sameDir bool, sameServer bool) {
	// Check if mountPath is NFS mounted and mounted on server:serverPath
	cmd := exec.Command("df", "--output=fstype", "--output=source", "--output=target", mountPath)
	out := new(bytes.Buffer)
	cmd.Stdout = out
	err := cmd.Run()
	if err != nil {
		fmt.Println("Failed to run command for mount information (df", mountPath, "):", err)
		return false, false, false
	} else {
		var fsType string
		var source string
		var target string
		n, sErr := fmt.Sscanf(out.String(), "Type Filesystem Mounted on\n%s %s %s\n", &fsType, &source, &target)
		if sErr != nil || n != 3 {
			fmt.Println("Failed to parse mount information for", mountPath, ":", n, err)
			return false, false, false
		}
		isNfs = strings.Contains(fsType, "nfs")
		sourceElems := strings.Split(source, ":")
		if len(sourceElems) == 2 {
			sameServer = sourceElems[0] == server
			// Check if the source dir is the same as or if it is a parent dir of serverPath
			sameDir = (sourceElems[1] == serverPath) || strings.HasPrefix(serverPath, fmt.Sprintf("%s/", sourceElems[1]))
		}
		return isNfs, sameDir, sameServer
	}
}

func ArrayDiff(before, after []string) ([]string, []string) {
	b := map[string]bool{}
	var added []string
	for _, v := range before {
		b[v] = true
	}
	for _, k := range after {
		if !b[k] {
			added = append(added, k)
		}
		delete(b, k)
	}
	var removed []string
	for k := range b {
		removed = append(removed, k)
	}
	sort.Strings(removed)
	sort.Strings(added)
	return removed, added
}

func GetUserConfirm(stdin io.Reader, stderr io.Writer) bool {
	reader := bufio.NewReader(stdin)
	for {
		_, _ = fmt.Fprint(stderr, "Continue? [y/n]: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)
		if input == "y" {
			return true
		} else if input == "n" {
			return false
		} else {
			_, _ = fmt.Fprintln(stderr, "Invalid input, please enter 'y' for yes or 'n' for no.")
		}
	}
}

type PreRunE func(cmd *cobra.Command, args []string) error
type ArgCheckPreRunE func(helpMsg string) cobra.PositionalArgs

func RequireNameArg(helpMsg string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("missing required NAME argument. " + helpMsg)
		}
		if len(args) > 1 {
			return errors.New("too many positional arguments. " + helpMsg)
		}
		return nil
	}
}

func DisallowPosArg(helpMsg string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != 0 {
			return errors.New("command takes no positional arguments. " + helpMsg)
		}
		return nil
	}
}
