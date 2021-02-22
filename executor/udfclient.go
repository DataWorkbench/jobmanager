package executor

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/udfpb"
	"google.golang.org/grpc"
)

type UdfClient struct {
	client udfpb.UdfmanagerClient
	ctx    context.Context
}

func NewUdfClient(serverAddr string) (c UdfClient, err error) {
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

	c.client = udfpb.NewUdfmanagerClient(conn)

	ln := glog.NewDefault().Clone()
	reqId, _ := idgenerator.New("").Take()
	ln.WithFields().AddString("rid", reqId)

	c.ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)

	return c, nil
}

func (s *UdfClient) DescribeUdfManager(ID string) (udfType string, name string, define string, err error) {
	var (
		req udfpb.DescribeRequest
		rep *udfpb.InfoReply
	)

	req.ID = ID
	rep, err = s.client.Describe(s.ctx, &req)
	if err != nil {
		return
	}
	udfType = rep.GetUdfType()
	name = rep.GetName()
	define = rep.GetDefine()

	return
}
