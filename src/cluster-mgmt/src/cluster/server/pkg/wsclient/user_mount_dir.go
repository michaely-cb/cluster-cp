package wsclient

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	"cerebras.com/job-operator/common"
)

/*
File contains an implementation of user-managed volumes. An end-user admin will modify a configmap containing
NFS volume configuration data. The end-user running an ML job can then specify one or more mounts on these
volumes through optional parameters to InitCompileJob and InitExecuteJob.
*/

// MountDirSpec is a trimmed down version of corev1.VolumeSource. This is the json object data value of the WorkerUserVolumes
// configmap.
// From /etc/fstab, we should see something like below:
// x.x.x.x:/tests /cb/tests nfs defaults 0 0
//
// When transformed into MountDirSpec, it should look something like this:
// { "type": "nfs", "containerPath": "/cb/tests", "server": "x.x.x.x", "serverPath": "/tests" }
//
// "containerPath" stands for the path in the container.
// For our purpose only, this should be exactly the same as the path on host of the user node.
//
// "serverPath" stands for the path on the NFS server.
// This path should be set up by sysadmins prior to running any jobs.
type MountDirSpec struct {
	// Type must be NFS/hostPath for now
	Type string `json:"type"`

	// ContainerPath is a local container path, which is the same as the host path
	ContainerPath string `json:"containerPath"`

	// Server is the IP or domain of NFS server
	Server string `json:"server"`

	// ServerPath is an NFS-mounted path
	ServerPath string `json:"serverPath"`

	// Readonly states whether the mount should be mounted readonly.
	Readonly bool `json:"readonly"`

	// Name of the volume
	Name string `json:"name"`

	// Labels of the volume
	Labels map[string]string `json:"labels"`
}

// UserMountDirDb is map of containerPath -> MountDirSpec. The user-managed volumes configmap is loaded into this object.
type UserMountDirDb map[string]MountDirSpec

func (vdb UserMountDirDb) parseCMData(data map[string]string) error {
	for k, v := range data {
		volJson := &MountDirSpec{}
		err := json.NewDecoder(strings.NewReader(v)).Decode(volJson)
		if err != nil {
			msg := fmt.Sprintf("%v Config map %s is corrupt: unable to parse volume mount %s=%s",
				wsapisv1.ContactSysAdminSupport, VolumesCmName, k, v)
			log.WithField("error", err).Warn(msg)
			return fmt.Errorf(msg)
		} else if volJson.Type != NFSVolumeType && volJson.Type != HostPathVolumeType {
			// As Dec 2022, NFS is the only volume type that is formally supported.
			// For internal testing purposes, we do support HostPath volume type, though it is not a formal offering.
			// Therefore, the error message below only mentions NFS as the supported volume type.
			msg := fmt.Sprintf("%v Config map %s is corrupt: contained volume with unknown type %s. "+
				"Cerebras currently only supports nfs.",
				wsapisv1.ContactSysAdminSupport, VolumesCmName, volJson.Type)
			log.Warn(msg)
			return fmt.Errorf(msg)
		}
		volJson.Name = k
		vdb[volJson.ContainerPath] = *volJson
	}
	return nil
}

// This function dynamically builds new volumes and the corresponding volume mounts.
// Every mount dir provided will yield its own volume and volume mount in the wsjob spec.
func (vdb UserMountDirDb) mountReq2VolumeMount(req *pb.MountDir) (*corev1.Volume, *corev1.VolumeMount, error) {
	var longestCharMatch = 0
	var longestContainerPathPrefix string
	vol := corev1.Volume{}

	for k, v := range vdb {
		if !strings.HasPrefix(req.GetPath(), k) || len(k) <= longestCharMatch {
			continue
		}
		longestCharMatch = len(k)
		longestContainerPathPrefix = k
		vol.Name = v.Name
		if v.Type == NFSVolumeType {
			vol = corev1.Volume{
				Name: v.Name,
				VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{
					Server:   v.Server,
					Path:     v.ServerPath,
					ReadOnly: v.Readonly,
				}},
			}
		} else {
			pathType := corev1.HostPathDirectoryOrCreate
			vol = corev1.Volume{
				Name: v.Name,
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: v.ContainerPath,
					Type: &pathType,
				}},
			}
		}
	}
	if longestCharMatch == 0 {
		return nil, nil, fmt.Errorf("use one of %v as path prefix", common.SortedKeys(vdb))
	}

	updatedVolName := fmt.Sprintf("%s-%s", vol.Name, GenUUID())
	vol.Name = updatedVolName
	if vol.VolumeSource.NFS != nil {
		// By this point, it is guaranteed that "longestContainerPathPrefix" is not empty and
		// should be the best matching volume to be applied.
		// We first find out the relative path after the longest container path prefix,
		// then append the relative path after the corresponding server path.
		vdbRelativePath, _ := filepath.Rel(longestContainerPathPrefix, req.GetPath())
		vol.VolumeSource.NFS.Path = filepath.Join(vol.VolumeSource.NFS.Path, vdbRelativePath)
	} else {
		vol.VolumeSource.HostPath.Path = req.GetPath()
	}
	vm := corev1.VolumeMount{
		Name:      updatedVolName,
		MountPath: req.GetContainerPath(),
	}
	// best-effort check path is valid
	if strings.Contains(vm.MountPath, "..") {
		return nil, nil, fmt.Errorf("VolumeMount.path '%s' is not allowed", vm.MountPath)
	}
	return &vol, &vm, nil
}

func GetUserMountDirs(ctx context.Context, namespace string, k8sClient kubernetes.Interface) (UserMountDirDb, error) {
	// TODO: update to use informer cache
	cm, err := k8sClient.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, VolumesCmName, metav1.GetOptions{})
	if err != nil && errors.IsNotFound(err) {
		return UserMountDirDb{}, nil
	}
	if err != nil {
		log.WithField("name", VolumesCmName).
			WithField("err", err).
			Error("failed to fetch volume mounts configmap")
		return nil, grpcStatus.Errorf(codes.Internal, err.Error())
	}

	volumeCfg := UserMountDirDb{}
	if err = volumeCfg.parseCMData(cm.Data); err != nil {
		return nil, grpcStatus.Errorf(codes.Internal, err.Error())
	}
	return volumeCfg, nil
}

func ResolveMountDirs(
	ctx context.Context,
	namespace string,
	k8sClient kubernetes.Interface,
	mountReqs []*pb.MountDir,
) ([]*corev1.Volume, []*corev1.VolumeMount, error) {
	if len(mountReqs) == 0 {
		return nil, nil, nil
	}

	volumeCfg, err := GetUserMountDirs(ctx, namespace, k8sClient)
	if err != nil {
		return nil, nil, err
	}
	if len(volumeCfg) == 0 {
		return nil, nil, grpcStatus.Errorf(codes.FailedPrecondition,
			"%v Volume mount failed: server has no user volumes configured.", wsapisv1.ContactSysAdminSupport)
	}
	var vols []*corev1.Volume
	var mounts []*corev1.VolumeMount
	for i, req := range mountReqs {
		// Users always pass in "Path" and at times would also pass in "ContainerPath".
		// If no "ContainerPath" was passed, we use the same "Path" value.
		if req.GetContainerPath() == "" {
			req.ContainerPath = req.GetPath()
		}
		volume, mount, err := volumeCfg.mountReq2VolumeMount(req)
		if err != nil {
			return nil, nil, grpcStatus.Errorf(codes.InvalidArgument, "%v Invalid volume mount '%v': %s",
				wsapisv1.ContactSysAdminSupport, mountReqs[i].GetPath(), err.Error())
		}
		vols = append(vols, volume)
		mounts = append(mounts, mount)
	}
	return vols, mounts, nil
}
