package service

import (
	"context"
	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	flinkService "github.com/DataWorkbench/jobmanager/service/flink"
	"github.com/DataWorkbench/jobmanager/utils"
	"gorm.io/gorm"
)

type JobManagerService struct {
	ctx            context.Context
	logger         *glog.Logger
	flinkExecutors map[model.StreamJob_Type]flinkService.Executor
}

func NewJobManagerService(ctx context.Context, db *gorm.DB, uClient utils.UdfClient, eClient utils.EngineClient,
	rClient utils.ResourceClient, logger *glog.Logger, zeppelinConfig zeppelin.ClientConfig,
	flinkConfig flink.ClientConfig) *JobManagerService {
	flinkBase := flinkService.NewBaseManager(ctx, db, logger, eClient, uClient, rClient, flinkConfig, zeppelinConfig)
	return &JobManagerService{
		ctx:            ctx,
		logger:         logger,
		flinkExecutors: createFlinkExecutor(ctx, flinkBase),
	}
}

func (jm *JobManagerService) RunFlinkJob(ctx context.Context, jobInfo *request.RunJob) (*response.RunJob, error) {
	res := response.RunJob{}
	executor := jm.flinkExecutors[jobInfo.Code.Type]

	result, err := executor.Run(ctx, jobInfo)
	if err != nil {
		return nil, err
	}
	data, state := flinkService.TransResult(result)
	res.Message = data
	res.State = state
	return &res, nil
}

func (jm *JobManagerService) PreRunFlinkJob(ctx context.Context, jobInfo *request.RunJob) error {
	return nil
}

func (jm *JobManagerService) ReleaseNote(ctx context.Context,jobType model.StreamJob_Type,instanceId string) error{
	executor := jm.flinkExecutors[jobType]
	return executor.Release(ctx,instanceId)
}

func (jm *JobManagerService) CancelFlinkJob(ctx context.Context, jobType model.StreamJob_Type, instanceId string, spaceId string, clusterId string) error {
	executor := jm.flinkExecutors[jobType]
	return executor.Cancel(ctx, instanceId, spaceId, clusterId)
}

func (jm *JobManagerService) GetFlinkJob(ctx context.Context, jobType model.StreamJob_Type, instanceId string, spaceId string, clusterId string) (*response.GetJobInfo, error) {
	res := response.GetJobInfo{}
	executor := jm.flinkExecutors[jobType]
	job, err := executor.GetInfo(ctx, instanceId, spaceId, clusterId)
	if err != nil {
		return nil, err
	}
	switch job.State {
	case "FAILED":
		res.State = model.StreamJobInst_Failed
	case "FAILING", "INITIALIZING", "RESTARTING", "RECONCILING", "CANCELLING":
		res.State = model.StreamJobInst_Pending
	case "CREATED", "FINISHED", "CANCELED", "SUSPENDED":
		res.State = model.StreamJobInst_Succeed
	case "RUNNING":
		res.State = model.StreamJobInst_Running
	}
	return &res, nil
}

func (jm *JobManagerService) ValidateCode(jobCode *model.StreamJobCode) (*response.StreamJobCodeSyntax, error) {
	res := response.StreamJobCodeSyntax{}
	executor := jm.flinkExecutors[jobCode.Type]
	if flag, msg, err := executor.Validate(jobCode); err != nil {
		return nil, err
	} else {
		if flag {
			res.Result = response.StreamJobCodeSyntax_Correct
		} else {
			res.Result = response.StreamJobCodeSyntax_Incorrect
			res.Message = msg
		}
		return &res, nil
	}
}

func createFlinkExecutor(ctx context.Context, bm *flinkService.BaseExecutor) map[model.StreamJob_Type]flinkService.Executor {
	executors := map[model.StreamJob_Type]flinkService.Executor{}
	executors[model.StreamJob_SQL] = flinkService.NewExecutor(ctx, model.StreamJob_SQL, bm)
	executors[model.StreamJob_Jar] = flinkService.NewExecutor(ctx, model.StreamJob_Jar, bm)
	executors[model.StreamJob_Scala] = flinkService.NewExecutor(ctx, model.StreamJob_Scala, bm)
	executors[model.StreamJob_Python] = flinkService.NewExecutor(ctx, model.StreamJob_Python, bm)
	return executors
}
