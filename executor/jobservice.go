package executor

import (
	"context"
	"strings"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/jobmanager/utils"
)

type JobManagerService struct {
	ctx            context.Context
	logger         *glog.Logger
	zeppelinConfig zeppelin.ClientConfig
	bm             *BaseManagerExecutor
}

const (
	FlinkCancelling   = "CANCELLING"
	FlinkReconciling  = "RECONCILING"
	FlinkSuspended    = "SUSPENDED"
	FlinkRunning      = "RUNNING"
	FlinkRestarting   = "RESTARTING"
	FlinkCanceled     = "CANCELED"
	FlinkFailed       = "FAILED"
	FlinkFinished     = "FINISHED"
	FlinkInitializing = "INITIALIZING"
	FlinkCreated      = "CREATED"
	FlinkFailing      = "FAILING"
)

func NewJobManagerService(ctx context.Context, uClient utils.UdfClient, eClient utils.EngineClient,
	rClient utils.ResourceClient, logger *glog.Logger, zeppelinConfig zeppelin.ClientConfig, flinkConfig flink.ClientConfig) *JobManagerService {
	return &JobManagerService{
		ctx:            ctx,
		logger:         logger,
		zeppelinConfig: zeppelinConfig,
		bm:             NewBaseManager(eClient, uClient, rClient, flinkConfig),
	}
}

func (jm *JobManagerService) RunJob(ctx context.Context, jobInfo *request.JobInfo) (res *response.JobInfo, err error) {
	executor := NewExecutor(jobInfo.Code.Type, jm.bm, jm.zeppelinConfig, ctx, jm.logger)
	result, err := executor.Run(ctx, jobInfo)
	if err != nil {
		return
	}
	switch result.Status {
	case zeppelin.RUNNING:
		res.State = model.StreamJobInst_Running
	case zeppelin.ABORT:
		res.State = model.StreamJobInst_Suspended
	case zeppelin.ERROR:
		res.State = model.StreamJobInst_Failed
	case zeppelin.FINISHED:
		res.State = model.StreamJobInst_Succeed
	}
	for _, r := range result.Results {
		if strings.EqualFold("TEXT", r.Type) {
			res.Data = r.Data
		}
	}
	if len(result.JobUrls) > 0 && result.JobUrls[0] != "" && len(result.JobUrls[0]) == 32 {
		res.JobId = result.JobUrls[0]
		return
	}
	res.State = model.StreamJobInst_Timeout
	return
}

func (jm *JobManagerService) CancelJob(ctx context.Context, jobType model.StreamJob_Type, jobId string, spaceId string, clusterId string) error {
	executor := NewExecutor(jobType, jm.bm, jm.zeppelinConfig, ctx, jm.logger)
	return executor.Cancel(ctx, jobId, spaceId, clusterId)
}

func (jm *JobManagerService) GetJobInfo(ctx context.Context, jobType model.StreamJob_Type, jobId string,
	jobName string, spaceId string, clusterId string) (*response.JobInfo, error) {
	res := response.JobInfo{}
	executor := NewExecutor(jobType, jm.bm, jm.zeppelinConfig, ctx, jm.logger)
	job, err := executor.GetInfo(ctx, jobId, jobName, spaceId, clusterId)
	if err != nil {
		return nil, err
	}
	switch job.State {
	case FlinkRunning:
		res.State = model.StreamJobInst_Running
	case FlinkCreated:
	case FlinkInitializing:
		res.State = model.StreamJobInst_Pending
	case FlinkRestarting:
	case FlinkReconciling:
		res.State = model.StreamJobInst_Retrying
	case FlinkFailing:
	case FlinkFailed:
		res.State = model.StreamJobInst_Failed
	case FlinkCancelling:
	case FlinkCanceled:
		res.State = model.StreamJobInst_Terminated
	case FlinkSuspended:
		res.State = model.StreamJobInst_Suspended
	case FlinkFinished:
		res.State = model.StreamJobInst_Succeed
	}
	res.JobId = job.Jid
	return &res, nil
}
