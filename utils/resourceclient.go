package utils

import (
	"context"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcspace"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
	"github.com/DataWorkbench/gproto/xgo/types/pbresponse"
)

type ResourceClient struct {
	client pbsvcspace.ResourceMetaClient
}

func NewResourceClient(conn *grpcwrap.ClientConn) (c ResourceClient, err error) {
	c.client = pbsvcspace.NewResourceMetaClient(conn)
	return c, nil
}

func (s *ResourceClient) GetFileById(ctx context.Context, id string) (name string, url string, err error) {
	var reply *pbresponse.DescribeFileMeta
	reply, err = s.client.DescribeFileMeta(ctx, &pbrequest.DescribeFileMeta{ResourceId: id})
	if err != nil {
		return
	}
	name = reply.Info.Name
	url = constants.GenResourceFilePath(reply.Info.SpaceId, reply.Info.Id, reply.Info.Version)
	return
}
