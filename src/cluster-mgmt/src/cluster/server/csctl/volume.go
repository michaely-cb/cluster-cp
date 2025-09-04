package csctl

import (
	"context"
	"fmt"
	"sort"

	"cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/kubernetes"

	"cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg/wsclient"
)

type Volume struct {
	volume wsclient.MountDirSpec
}

func (v *Volume) Proto() proto.Message {
	return MapVolume(v.volume)
}

type VolumeList struct {
	volumes []wsclient.MountDirSpec
}

func (v *VolumeList) Proto() proto.Message {
	return &csv1.VolumeList{Items: common.Map(MapVolume, v.volumes)}
}

type VolumeStore struct {
	kubeClient kubernetes.Interface
}

func (v VolumeStore) ListV2(ctx context.Context, namespace string, options *csctl.ListOptions) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "ListV2 not implemented for this type")
}

func (v VolumeStore) Get(ctx context.Context, namespace, name string) (Resource, error) {
	// Currently, we allow all users to see the volumes.
	volumes, err := wsclient.GetUserMountDirs(ctx, namespace, v.kubeClient)
	if err != nil {
		return nil, err
	}
	for _, v := range volumes {
		if v.Name == name {
			return &Volume{volume: v}, nil
		}
	}
	return nil, grpcStatus.Errorf(codes.NotFound, "volume with name '%s' was not found", name)
}

func (v VolumeStore) List(ctx context.Context, namespace string, options *csctl.GetOptions) (Resource, error) {
	if options != nil {
		log.WithField("options", options).Warn("options argument is ignored")
	}
	// Currently, we allow all users to see the volumes.
	volumes, err := wsclient.GetUserMountDirs(ctx, namespace, v.kubeClient)
	if err != nil {
		return nil, err
	}
	nameToContainerPathMap := map[string]string{}
	for k, v := range volumes {
		nameToContainerPathMap[v.Name] = k
	}
	names := common.Keys(nameToContainerPathMap)
	sort.Strings(names)
	var rv []wsclient.MountDirSpec
	for _, name := range names {
		rv = append(rv, volumes[nameToContainerPathMap[name]])
	}
	return &VolumeList{volumes: rv}, nil
}

func (v VolumeStore) PatchMerge(ctx context.Context, namespace, name string, patch []byte) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "patch not implemented for this type")
}

func (v VolumeStore) AsTable(resource Resource) *csv1.Table {
	switch val := resource.(type) {
	case *Volume:
		table := emptyVolumeTablePb()
		table.Rows = []*csv1.RowData{mapVolumeTableRow(MapVolume(val.volume))}
		return table
	case *VolumeList:
		table := emptyVolumeTablePb()
		table.Rows = common.Map(mapVolumeTableRow, common.Map(MapVolume, val.volumes))
		return table
	default:
		panic(fmt.Sprintf("unhandled table conversion for type: %T", val))
	}
}

func emptyVolumeTablePb() *csv1.Table {
	return &csv1.Table{Columns: []*csv1.ColumnDefinition{
		{Name: "name"},
		{Name: "type"},
		{Name: "containerPath"},
		{Name: "server"},
		{Name: "serverPath"},
		{Name: "readonly"},
		{Name: "labels"},
	}}
}

func MapVolume(spec wsclient.MountDirSpec) *csv1.Volume {
	vol := &csv1.Volume{
		Meta: &csv1.ObjectMeta{
			Type:       "volume",
			Name:       spec.Name,
			Labels:     spec.Labels,
			CreateTime: &timestamppb.Timestamp{},
			FieldPriority: map[string]int32{
				"meta.fieldPriority": 2,
				"meta.createTime":    2,
			},
		},
	}
	if spec.Type == wsclient.NFSVolumeType {
		vol.VolumeSource = &csv1.Volume_Nfs{
			Nfs: &csv1.Volume_NFSVolume{
				ContainerPath: spec.ContainerPath,
				Server:        spec.Server,
				ServerPath:    spec.ServerPath,
				Readonly:      spec.Readonly,
			},
		}
	} else if spec.Type == wsclient.HostPathVolumeType {
		vol.VolumeSource = &csv1.Volume_HostPath{
			HostPath: &csv1.Volume_HostPathVolume{
				ContainerPath: spec.ContainerPath,
			},
		}
	} else {
		log.WithField("volume", spec).Warn("unable to serialize unknown volume type")
	}
	return vol
}

func mapVolumeTableRow(volume *csv1.Volume) *csv1.RowData {
	cells := []string{
		volume.Meta.Name, "unknown", "", "", "", "", "",
	}
	if volume.GetNfs() != nil {
		nfs := volume.GetNfs()
		cells = []string{
			volume.Meta.Name,
			wsclient.NFSVolumeType,
			nfs.ContainerPath,
			nfs.Server,
			nfs.ServerPath,
			fmt.Sprintf("%v", nfs.Readonly),
			serializeLabels(volume.Meta.Labels),
		}
	} else if volume.GetHostPath() != nil {
		hostPath := volume.GetHostPath()
		cells = []string{
			volume.Meta.Name,
			wsclient.HostPathVolumeType,
			hostPath.ContainerPath,
			"",
			"",
			"false",
			serializeLabels(volume.Meta.Labels),
		}
	}
	return &csv1.RowData{Cells: cells}
}

func serializeLabels(labels map[string]string) string {
	serialized := ""
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if serialized == "" {
			serialized += fmt.Sprintf("%s=%s", k, labels[k])
		} else {
			serialized += fmt.Sprintf(",%s=%s", k, labels[k])
		}
	}
	return serialized
}
