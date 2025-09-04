package common

import (
	"path"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// DedupeMountDirVolumes dedupes identical or lower-level mount dir volumes and mounts.
// K8s enforces the following set of rules:
//   - specifying the same volume multiple times is not allowed
//   - specifying the same mount path in multiple volume mounts is not allowed
//   - specifying a stricter (700) lower-level volume along with a relaxed (777) higher-level
//     volume is not allowed in root-squashed NFS environments, even though security context
//     had access to both volumes
//
// The reason why we have to explicitly dedupe is mainly due to the fact all mount dirs are
// user requests and there are different interfaces where users could specify them. Meanwhile,
// operator also explicitly relies on presence of the workdir volume to perform proper volume
// and mount hydration. This makes it impossible to perform volume/mount consolidation at
// the server side only.
func DedupeMountDirVolumes(podTemplate *corev1.PodTemplateSpec) {
	numInitContainers := len(podTemplate.Spec.InitContainers)
	sanitizedContainers := append(podTemplate.Spec.InitContainers, podTemplate.Spec.Containers...)

	// For example: sanitizedVolumeMap["x.x.x.x"] = {"/tests/abc": "vol1", "/tests/xyz": "vol2"}
	// Idea is that any path key should not be a subpath of any other path key in the same server
	sanitizedVolumeMap := map[string]map[string]string{}
	type vvmPair struct {
		volume corev1.Volume
		mount  corev1.VolumeMount
	}
	vvmConfig := map[string]vvmPair{}

	// For example:
	// - relaxPlan["lower-volume"] = "higher-volume"
	// - relaxPlan["higher-volume"] = "higher-volume"
	relaxPlan := map[string]string{}

	var sanitizedVolumes []corev1.Volume
	for _, newVol := range podTemplate.Spec.Volumes {
		// skip volumes that are not nfs or hostpath
		if newVol.NFS == nil && newVol.HostPath == nil {
			sanitizedVolumes = append(sanitizedVolumes, newVol)
			continue
		}

		// skip volumes if the corresponding mounts involved subpath for backward compatibility
		subpathInvolved := false
		newVolMount := corev1.VolumeMount{}
		for _, c := range sanitizedContainers {
			for _, vm := range c.VolumeMounts {
				if vm.Name == newVol.Name {
					if vm.SubPath != "" || vm.SubPathExpr != "" {
						subpathInvolved = true
					}
					newVolMount = vm
					break
				}
			}
		}
		if subpathInvolved {
			sanitizedVolumes = append(sanitizedVolumes, newVol)
			continue
		}

		newVolServer := ""
		newVolPath := ""
		if newVol.NFS != nil {
			newVolServer = newVol.NFS.Server
			newVolPath = newVol.NFS.Path
		} else {
			newVolPath = newVol.HostPath.Path
		}

		sanitizedVolumeMap[newVolServer] = EnsureMap(sanitizedVolumeMap[newVolServer])
		if volSeen, ok := sanitizedVolumeMap[newVolServer][newVolPath]; ok {
			// newVol is an identical volume, update all mounts to point to the volume seen
			volToKeep := Ternary(volSeen < newVol.Name, volSeen, newVol.Name)
			volToDelete := Ternary(volSeen >= newVol.Name, volSeen, newVol.Name)
			for i := range sanitizedContainers {
				for j := 0; j < len(sanitizedContainers[i].VolumeMounts); j++ {
					if sanitizedContainers[i].VolumeMounts[j].Name == volToDelete {
						sanitizedContainers[i].VolumeMounts[j].Name = volToKeep
					}
				}
			}
			if volToKeep == volSeen {
				continue
			}
		}

		sanitizedVolumeMap[newVolServer][newVolPath] = newVol.Name
		vvmConfig[newVol.Name] = vvmPair{
			volume: newVol,
			mount:  newVolMount,
		}
	}

	for server := range sanitizedVolumeMap {
		sortedPaths := SortedKeys(sanitizedVolumeMap[server])
		if len(sortedPaths) == 0 {
			continue
		}

		relaxedVolumeName := ""
		lastHighLevelPath := ""
		for i := 0; i < len(sortedPaths); i++ {
			volName := sanitizedVolumeMap[server][sortedPaths[i]]
			// this is a new high volume, if one of the following is true:
			// - this is a hostpath volume
			// - there was no relaxed volume identified
			// - the current volume path is not a subpath of the relaxed volume path
			// - the current volume mount path is not a subpath of the relaxed volume mount path
			if vvmConfig[volName].volume.HostPath != nil ||
				relaxedVolumeName == "" ||
				!isSubPath(lastHighLevelPath, sortedPaths[i]) ||
				!isSubPath(vvmConfig[relaxedVolumeName].mount.MountPath, vvmConfig[volName].mount.MountPath) {
				relaxedVolumeName = volName
				lastHighLevelPath = sortedPaths[i]
				sanitizedVolumes = append(sanitizedVolumes, vvmConfig[volName].volume)
			}
			relaxPlan[volName] = relaxedVolumeName
		}
	}

	for i := range sanitizedContainers {
		var sanitizedVolumeMounts []corev1.VolumeMount
		uniqueVMs := map[string]bool{}
		for j := range sanitizedContainers[i].VolumeMounts {
			originalVm := sanitizedContainers[i].VolumeMounts[j]
			if relaxedVmName, ok := relaxPlan[originalVm.Name]; !ok {
				// if not found in relaxPlan, it means the corresponding volume was skipped at the beginning
				sanitizedVolumeMounts = append(sanitizedVolumeMounts, originalVm)
			} else if _, ok = uniqueVMs[relaxedVmName]; !ok {
				uniqueVMs[relaxedVmName] = true
				sanitizedVolumeMounts = append(sanitizedVolumeMounts, vvmConfig[relaxedVmName].mount)
			}
		}
		sanitizedContainers[i].VolumeMounts = sanitizedVolumeMounts
	}

	podTemplate.Spec.InitContainers = sanitizedContainers[:numInitContainers]
	podTemplate.Spec.Containers = sanitizedContainers[numInitContainers:]
	podTemplate.Spec.Volumes = sanitizedVolumes
}

func isSubPath(parent, child string) bool {
	parent = path.Clean(parent)
	child = path.Clean(child)

	if !strings.HasSuffix(parent, "/") {
		parent += "/"
	}

	return strings.HasPrefix(child, parent)
}
