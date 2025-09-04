package wsclient

import (
	"context"
	"path"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	VolumesCmName      = "cluster-server-volumes"
	NFSVolumeType      = "nfs"
	HostPathVolumeType = "hostPath"
)

func GetWorkdirVolumeAndMount(ctx context.Context, k8s kubernetes.Interface, jobSpec *JobSpec, volumeRootPath string) error {
	// jobSpec.WorkdirLogsRootPath should be the workdir root, for example:
	// - /n1/wsjob/workdir
	// - /cb/tests/cluster-mgmt/mbx/workdir (internal nfs root)
	// - /home/user-x (user provided nfs root through debug args)
	lowerDir := path.Join(jobSpec.Namespace, jobSpec.JobName)
	var err error
	if volumeRootPath == "" {
		jobSpec.WorkdirLogsRootPath = jobSpec.JobPathConfig.LocalWorkdirLogsRootPath
		jobSpec.WorkdirLogPath = path.Join(jobSpec.WorkdirLogsRootPath, lowerDir)
		jobSpec.WorkdirLogsVolume = BuildHostPathVolume(wsapisv1.WorkdirVolume, jobSpec.WorkdirLogsRootPath, corev1.HostPathDirectory)
		jobSpec.WorkdirLogsVolumeMount = &corev1.VolumeMount{
			Name:        wsapisv1.WorkdirVolume,
			MountPath:   path.Join(jobSpec.WorkdirLogsRootPath, lowerDir),
			SubPathExpr: lowerDir,
		}
	} else {
		jobSpec.WorkdirLogsRootPath = volumeRootPath
		jobSpec.WorkdirLogPath = path.Join(jobSpec.WorkdirLogsRootPath, lowerDir)
		jobSpec.Annotations = wscommon.EnsureMap(jobSpec.Annotations)
		jobSpec.Annotations[wsapisv1.WorkdirLogsMountDirAnnot] = jobSpec.WorkdirLogPath
		jobSpec.SetInfraSetupIfRequired()

		jobSpec.WorkdirLogsVolume, jobSpec.WorkdirLogsVolumeMount, err = getMountDirVolumeAndMount(
			ctx, k8s, jobSpec.Namespace, wsapisv1.WorkdirVolume, volumeRootPath, lowerDir, jobSpec.IsInfraSetupRequired())
	}
	return err
}

func GetCachedCompileVolumeAndMount(ctx context.Context, k8s kubernetes.Interface, jobSpec *JobSpec,
	volumeRootPath string, hasMultiMgmtNodes bool) error {
	// rootMountPath should be the compile root, for example:
	// - /n1/wsjob/compile_root
	// - /cb/tests/cluster-mgmt/compile_root (internal nfs root)
	// - /home/user-x (user provided nfs root through debug args)
	lowerDir := path.Join(jobSpec.Namespace, jobSpec.CachedCompileRelativePath)
	var err error
	if volumeRootPath == "" {
		jobSpec.CachedCompileRootPath = jobSpec.JobPathConfig.CachedCompileRootPath
		if hasMultiMgmtNodes {
			jobSpec.CachedCompileVolume = BuildPVCVolume(wsapisv1.CachedCompileVolume, wsapisv1.CachedCompilePVCName)
		} else {
			jobSpec.CachedCompileVolume = BuildHostPathVolume(wsapisv1.CachedCompileVolume, jobSpec.CachedCompileRootPath, corev1.HostPathDirectory)
		}
		jobSpec.CachedCompileVolumeMount = &corev1.VolumeMount{
			Name:        wsapisv1.CachedCompileVolume,
			MountPath:   path.Join(jobSpec.CachedCompileRootPath, lowerDir),
			SubPathExpr: lowerDir,
		}
	} else {
		jobSpec.CachedCompileRootPath = volumeRootPath
		jobSpec.Annotations = wscommon.EnsureMap(jobSpec.Annotations)
		jobSpec.Annotations[wsapisv1.CachedCompileMountDirAnnot] = path.Join(jobSpec.CachedCompileRootPath, lowerDir)
		jobSpec.SetInfraSetupIfRequired()
		jobSpec.CachedCompileVolume, jobSpec.CachedCompileVolumeMount, err = getMountDirVolumeAndMount(
			ctx, k8s, jobSpec.Namespace, wsapisv1.CachedCompileVolume, volumeRootPath, lowerDir, jobSpec.IsInfraSetupRequired())
	}
	return err
}

func getMountDirVolumeAndMount(ctx context.Context, k8s kubernetes.Interface, ns, volumeName, mountDirRootPath, lowerDir string, infraSetupRequired bool) (
	*corev1.Volume, *corev1.VolumeMount, error) {
	volume := &corev1.Volume{}
	mount := &corev1.VolumeMount{}
	volumes, mounts, err := ResolveMountDirs(ctx, ns, k8s, []*pb.MountDir{{
		Path:          mountDirRootPath,
		ContainerPath: mountDirRootPath,
	}})
	if err != nil {
		return volume, mount, err
	}

	volume = volumes[0]
	volume.Name = volumeName
	mount = mounts[0]
	mount.Name = volumeName
	mount.MountPath = path.Join(mountDirRootPath, lowerDir)
	mount.SubPathExpr = lowerDir

	// If operator supports infra setup for NFS, we prefer to set up the volume mount
	// without using SubPath/SubPathExpr. When SubPath/SubPathExpr is used, kubelet
	// always validates whether the subpath exists and creates if it does not exist.
	// Even if the subpath had already been created, if kubelet did not have permissions
	// to read/execute in any part of the subpath, the pod creation would fail with
	// config error. Note that kubelet (owned by root in our clusters) is `nobody` in
	// root-squashed NFS. So for subpath to work properly, we would have to relax the
	// directory permission for everyone to be able to read and execute, which is undesired.
	if infraSetupRequired {
		mount.SubPathExpr = ""
		if volume.NFS != nil {
			volume.NFS.Path = path.Join(volume.NFS.Path, lowerDir)
		} else if volume.HostPath != nil {
			// for test purposes only
			volume.HostPath.Path = path.Join(volume.HostPath.Path, lowerDir)
		}
	}
	return volume, mount, err
}

func nfsMountDirSpec(name, containerPath, server, serverPath string, readonly bool) *MountDirSpec {
	return &MountDirSpec{
		Name:          name,
		Type:          NFSVolumeType,
		ContainerPath: containerPath,
		Server:        server,
		ServerPath:    serverPath,
		Readonly:      readonly,
	}
}

func hostPathMountDirSpec(name, containerPath string, readonly bool) *MountDirSpec {
	return &MountDirSpec{
		Name:          name,
		Type:          HostPathVolumeType,
		ContainerPath: containerPath,
		Readonly:      readonly,
	}
}

func BuildPVCVolume(volumeName, pvcName string) *corev1.Volume {
	return &corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
				ReadOnly:  false,
			},
		},
	}
}

func BuildHostPathVolume(volumeName, hostPath string, hostPathType corev1.HostPathType) *corev1.Volume {
	return &corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: hostPath,
				Type: &hostPathType,
			},
		},
	}
}

func BuildNfsVolume(volumeName, server, serverPath string, readonly bool) *corev1.Volume {
	return &corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server:   server,
				Path:     serverPath,
				ReadOnly: readonly,
			},
		},
	}
}
