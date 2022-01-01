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

func (jm *JobManagerService) RunJob(ctx context.Context, jobInfo *request.JobInfo) (*response.JobInfo, error) {
	res := response.JobInfo{}
	executor := NewExecutor(jobInfo.Code.Type, jm.bm, jm.zeppelinConfig, ctx, jm.logger)
	result, err := executor.Run(ctx, jobInfo)
	if err != nil {
		return &res, err
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
	}
	job, err := executor.GetInfo(ctx, res.JobId, jobInfo.JobId, jobInfo.SpaceId, jobInfo.Args.ClusterId)
	if err != nil {
		jm.logger.Warn().Msg(err.Error()).Fire()
	} else if len(job.Jid) == 32 {
		res.JobId = job.Jid
		res.State = transFlinkState(job.State)
	} else if len(res.JobId) != 32 {
		res.State = model.StreamJobInst_Timeout
	}
	return &res, nil
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
	res.State = transFlinkState(job.State)
	res.JobId = job.Jid
	return &res, nil
}

func transFlinkState(state string) model.StreamJobInst_State {
	switch state {
	case FlinkRunning:
		return model.StreamJobInst_Running
	case FlinkCreated:
	case FlinkInitializing:
		return model.StreamJobInst_Pending
	case FlinkRestarting:
	case FlinkReconciling:
		return model.StreamJobInst_Retrying
	case FlinkFailing:
	case FlinkFailed:
		return model.StreamJobInst_Failed
	case FlinkCancelling:
	case FlinkCanceled:
		return model.StreamJobInst_Terminated
	case FlinkSuspended:
		return model.StreamJobInst_Suspended
	case FlinkFinished:
		return model.StreamJobInst_Succeed
	}
	return model.StreamJobInst_Timeout
}
