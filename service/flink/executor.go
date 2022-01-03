package flink

import (
	"context"
	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type Executor interface {
	Run(ctx context.Context, info *request.JobInfo) (*zeppelin.ExecuteResult, error)
	Cancel(ctx context.Context, jobId string, spaceId string, clusterId string) error
	GetInfo(ctx context.Context, jobId string, jobName string, spaceId string, clusterId string) (*flink.Job, error)
}

func NewExecutor(ctx context.Context, jobType model.StreamJob_Type, bm *BaseExecutor) Executor {
	var executor Executor
	switch jobType {
	case model.StreamJob_SQL:
		executor = NewSqlExecutor(ctx, bm)
	case model.StreamJob_Jar:
		executor = NewJarExecutor(bm, ctx)
	case model.StreamJob_Python:
		executor = NewPythonExecutor(bm, ctx)
	case model.StreamJob_Scala:
		executor = NewScalaExecutor(bm, ctx)
	default:
	}
	return executor
}
