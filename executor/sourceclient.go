package executor

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/smpb"
	"google.golang.org/grpc"
)

type SourceClient struct {
	client smpb.SourcemanagerClient
	ctx    context.Context
}

func NewSourceClient(serverAddr string) (c SourceClient, err error) {
	var conn *grpc.ClientConn

	ctx := glog.WithContext(context.Background(), glog.NewDefault())
	conn, err = grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address:      serverAddr,
		LogLevel:     2,
		LogVerbosity: 99,
	})
	if err != nil {
		return
	}

	c.client = smpb.NewSourcemanagerClient(conn)

	ln := glog.NewDefault().Clone()
	reqId, _ := idgenerator.New("").Take()
	ln.WithFields().AddString("rid", reqId)

	c.ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)

	return c, nil
}

func (s *SourceClient) DescribeSourceTable(ID string) (sourceID string, tableName string, url string, err error) {
	var (
		req smpb.SotDescribeRequest
		rep *smpb.SotInfoReply
	)

	req.ID = ID
	rep, err = s.client.SotDescribe(s.ctx, &req)
	if err != nil {
		return
	}
	tableName = rep.GetName()
	sourceID = rep.GetSourceID()
	url = rep.GetUrl()

	return
}

func (s *SourceClient) DescribeSourceManager(ID string) (sourceType string, url string, err error) {
	var (
		req smpb.DescribeRequest
		rep *smpb.InfoReply
	)

	req.ID = ID
	rep, err = s.client.Describe(s.ctx, &req)
	if err != nil {
		return
	}
	sourceType = rep.GetSourceType()
	url = rep.GetUrl()

	return
}
