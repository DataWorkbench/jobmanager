package utils

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcresource"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
)

type ResourceClient struct {
	client pbsvcresource.ResourceManageClient
}

func NewResourceClient(conn *grpcwrap.ClientConn) (c ResourceClient, err error) {
	c.client = pbsvcresource.NewResourceManageClient(conn)
	return c, nil
}

func (s *ResourceClient) GetFileById(ctx context.Context, id string) (name string, url string, err error) {
	res, err := s.client.DescribeFile(ctx, &pbrequest.DescribeFile{ResourceId: id})
	if err != nil {
		return
	}
	name = res.Name
	spaceId := res.SpaceId
	url = "/" + spaceId + "/" + id + ".jar"
	return
}
