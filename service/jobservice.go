package service

import (
	"context"
	"strings"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	flinkService "github.com/DataWorkbench/jobmanager/service/flink"
	"github.com/DataWorkbench/jobmanager/utils"
)

type JobManagerService struct {
	ctx            context.Context
	logger         *glog.Logger
	zeppelinConfig zeppelin.ClientConfig
	flinkExecutors map[model.StreamJob_Type]flinkService.Executor
}

func NewJobManagerService(ctx context.Context, uClient utils.UdfClient, eClient utils.EngineClient,
	rClient utils.ResourceClient, logger *glog.Logger, zeppelinConfig zeppelin.ClientConfig, flinkConfig flink.ClientConfig) *JobManagerService {
	jobManager := JobManagerService{}
	jobManager.ctx = ctx
	jobManager.logger = logger

	jobManager.flinkExecutors = jobManager.createFlinkExecutor(ctx, flinkService.NewBaseManager(eClient, uClient, rClient, flinkConfig, zeppelinConfig), logger)

	return &jobManager
}

func (jm *JobManagerService) RunFlinkJob(ctx context.Context, jobInfo *request.JobInfo) (*response.JobInfo, error) {
	res := response.JobInfo{}
	executor := jm.getFlinkExecutor(jobInfo.Code.Type)
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
		res.State = jm.transFlinkState(job.State)
	} else if len(res.JobId) != 32 {
		res.State = model.StreamJobInst_Timeout
	}
	return &res, nil
}

func (jm *JobManagerService) CancelFlinkJob(ctx context.Context, jobType model.StreamJob_Type, jobId string, spaceId string, clusterId string) error {
	executor := jm.getFlinkExecutor(jobType)
	return executor.Cancel(ctx, jobId, spaceId, clusterId)
}

func (jm *JobManagerService) GetFlinkJob(ctx context.Context, jobType model.StreamJob_Type, jobId string,
	jobName string, spaceId string, clusterId string) (*response.JobInfo, error) {
	res := response.JobInfo{}
	executor := jm.getFlinkExecutor(jobType)
	job, err := executor.GetInfo(ctx, jobId, jobName, spaceId, clusterId)
	if err != nil {
		return nil, err
	}
	res.State = jm.transFlinkState(job.State)
	res.JobId = job.Jid
	return &res, nil
}

func (jm *JobManagerService) createFlinkExecutor(ctx context.Context, bm *flinkService.BaseExecutor, logger *glog.Logger) map[model.StreamJob_Type]flinkService.Executor {
	executors := map[model.StreamJob_Type]flinkService.Executor{}
	executors[model.StreamJob_SQL] = flinkService.NewExecutor(ctx, model.StreamJob_SQL, bm, logger)
	executors[model.StreamJob_Jar] = flinkService.NewExecutor(ctx, model.StreamJob_Jar, bm, logger)
	executors[model.StreamJob_Scala] = flinkService.NewExecutor(ctx, model.StreamJob_Scala, bm, logger)
	return executors
}

func (jm *JobManagerService) getFlinkExecutor(jobType model.StreamJob_Type) flinkService.Executor {
	return jm.flinkExecutors[jobType]
}

func (jm *JobManagerService) transFlinkState(state string) model.StreamJobInst_State {
	const (
		Cancelling   = "CANCELLING"
		Reconciling  = "RECONCILING"
		Suspended    = "SUSPENDED"
		Running      = "RUNNING"
		Restarting   = "RESTARTING"
		Canceled     = "CANCELED"
		Failed       = "FAILED"
		Finished     = "FINISHED"
		Initializing = "INITIALIZING"
		Created      = "CREATED"
		Failing      = "FAILING"
	)
	switch state {
	case Running:
		return model.StreamJobInst_Running
	case Created:
	case Initializing:
		return model.StreamJobInst_Pending
	case Restarting:
	case Reconciling:
		return model.StreamJobInst_Retrying
	case Failing:
	case Failed:
		return model.StreamJobInst_Failed
	case Cancelling:
	case Canceled:
		return model.StreamJobInst_Terminated
	case Suspended:
		return model.StreamJobInst_Suspended
	case Finished:
		return model.StreamJobInst_Succeed
	}
	return model.StreamJobInst_Timeout
}
