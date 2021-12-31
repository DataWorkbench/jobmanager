package utils

import (
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/enginepb"
)

type EngineClient struct {
	Client enginepb.EngineClient
}

func NewEngineClient(conn *grpcwrap.ClientConn) (c EngineClient, err error) {
	c.Client = enginepb.NewEngineClient(conn)
	return c, nil
}
