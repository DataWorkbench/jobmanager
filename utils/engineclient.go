package utils

import (
	"context"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/enginepb"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type EngineClient struct {
	Client enginepb.EngineClient
}

func NewEngineClient(conn *grpcwrap.ClientConn) (c EngineClient, err error) {
	c.Client = enginepb.NewEngineClient(conn)
	return c, nil
}

func (e EngineClient) GetEngineInfo(ctx context.Context, spaceId string, clusterId string) (url string, version string, err error) {
	api, err := e.Client.DescribeFlinkClusterAPI(ctx, &request.DescribeFlinkClusterAPI{
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
