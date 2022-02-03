package utils

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcengine"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
)

type EngineClient struct {
	Client pbsvcengine.EngineClient
}

func NewEngineClient(conn *grpcwrap.ClientConn) (c EngineClient, err error) {
	c.Client = pbsvcengine.NewEngineClient(conn)
	return c, nil
}

func (e EngineClient) GetEngineInfo(ctx context.Context, spaceId string, clusterId string) (url string, version string, err error) {
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
