package executor

import (
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/enginepb"
)

type EngineClient struct {
	client enginepb.EngineClient
}

func NewEngineClient(conn *grpcwrap.ClientConn) (c EngineClient, err error) {
	c.client = enginepb.NewEngineClient(conn)
	return c, nil
}
