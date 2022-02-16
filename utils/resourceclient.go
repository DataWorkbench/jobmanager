package utils

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/lib/storeio"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcspace"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
	"github.com/DataWorkbench/gproto/xgo/types/pbresponse"
)

type ResourceClient struct {
	client pbsvcspace.FileMetaManageClient
}

func NewResourceClient(conn *grpcwrap.ClientConn) (c ResourceClient, err error) {
	c.client = pbsvcspace.NewFileMetaManageClient(conn)
	return c, nil
}

func (s *ResourceClient) GetFileById(ctx context.Context, id string) (name string, url string, err error) {
	var reply *pbresponse.DescribeFileMeta
	reply, err = s.client.DescribeFileMeta(ctx, &pbrequest.DescribeFileMeta{FileId: id})
	if err != nil {
		return
	}
	name = reply.Info.Name
	url = storeio.GenerateFilePath(reply.Info.SpaceId, reply.Info.Id, reply.Info.Version)
	return
}
