package utils

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcspace"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
)

type ClusterManagerClient struct {
	Client pbsvcspace.ClusterManageClient
}

func NewClusterManagerClient(conn *grpcwrap.ClientConn) (c ClusterManagerClient, err error) {
	c.Client = pbsvcspace.NewClusterManageClient(conn)
	return c, nil
}

func (e ClusterManagerClient) GetEngineInfo(ctx context.Context, spaceId string, clusterId string) (url string, version string, err error) {
	api, err := e.Client.DescribeFlinkClusterAPI(ctx, &pbrequest.DescribeFlinkClusterAPI{
		SpaceId:   spaceId,
		ClusterId: clusterId,
	})
	if err != nil {
		return
	}
	url = api.URL
	version = api.Version
	return
}
