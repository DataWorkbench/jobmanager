package utils

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcspace"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
	"github.com/DataWorkbench/gproto/xgo/types/pbresponse"
)

type UdfClient struct {
	client pbsvcspace.UDFManageClient
}

func NewUdfClient(conn *grpcwrap.ClientConn) (c UdfClient, err error) {
	c.client = pbsvcspace.NewUDFManageClient(conn)
	return c, nil
}

func (s *UdfClient) DescribeUdfManager(ctx context.Context, ID string) (udfLanguage pbmodel.UDF_Language, name string, define string, err error) {
	var (
		req  pbrequest.DescribeUDF
		resp *pbresponse.DescribeUDF
	)

	req.UdfId = ID
	resp, err = s.client.DescribeUDF(ctx, &req)
	if err != nil {
		return
	}
	udfLanguage = resp.Info.Language
	name = resp.Info.GetName()
	define = resp.Info.GetDefine()

	return
}
