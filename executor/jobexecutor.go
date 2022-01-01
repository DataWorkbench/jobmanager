package executor

import (
	"context"
	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type JobExecutor interface {
	Run(ctx context.Context, info *request.JobInfo) (*zeppelin.ExecuteResult, error)
	Cancel(ctx context.Context, jobId string, spaceId string, clusterId string) error
	GetInfo(ctx context.Context, jobId string, jobName string, spaceId string, clusterId string) (*flink.Job, error)
}

func NewExecutor(jobType model.StreamJob_Type, bm *BaseManagerExecutor, config zeppelin.ClientConfig, ctx context.Context, logger *glog.Logger) JobExecutor {
	var executor JobExecutor
	switch jobType {
	case model.StreamJob_SQL:
		executor = NewSqlManagerExecutor(bm, config, ctx, logger)
	case model.StreamJob_Jar:
		executor = NewJarManagerExecutor(bm, config, ctx, logger)
	case model.StreamJob_Python:
	case model.StreamJob_Scala:
		executor = NewScalaManagerExecutor(bm, config, ctx, logger)
	default:
	}
	return executor
}
