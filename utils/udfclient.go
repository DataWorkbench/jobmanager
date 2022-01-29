package utils

import (
	"context"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/gproto/pkg/service/pbsvcudf"
	"github.com/DataWorkbench/gproto/pkg/types/pbmodel"
	"github.com/DataWorkbench/gproto/pkg/types/pbrequest"
	"github.com/DataWorkbench/gproto/pkg/types/pbresponse"
)

type UdfClient struct {
	client pbsvcudf.UdfManageClient
}

func NewUdfClient(conn *grpcwrap.ClientConn) (c UdfClient, err error) {
	c.client = pbsvcudf.NewUdfManageClient(conn)
	return c, nil
}

func (s *UdfClient) DescribeUdfManager(ctx context.Context, ID string) (udfLanguage pbmodel.UDFInfo_Language, name string, define string, err error) {
	var (
		req  pbrequest.DescribeUDF
		resp *pbresponse.DescribeUDF
	)

	req.UdfId = ID
	resp, err = s.client.Describe(ctx, &req)
	if err != nil {
		return
	}
	udfLanguage = resp.Info.GetUdfLanguage()
	name = resp.Info.GetName()
	define = resp.Info.GetDefine()

	return
}
