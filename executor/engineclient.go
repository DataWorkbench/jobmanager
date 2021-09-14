package executor

import (
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/enginepb"
)

type EngineClient struct {
	client enginepb.FlinkEngineServiceClient
}

func NewEngineClient(conn *grpcwrap.ClientConn) (c EngineClient, err error) {
	c.client = enginepb.NewFlinkEngineServiceClient(conn)
	return c, nil
}
