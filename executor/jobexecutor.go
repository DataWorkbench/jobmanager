package executor

import (
	"context"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type JobExecutor interface {
	Run(ctx context.Context, info *request.JobInfo) (*zeppelin.ExecuteResult, error)
}
