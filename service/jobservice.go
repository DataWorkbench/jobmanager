package service

import (
	"context"
	"gorm.io/gorm"
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
	flinkExecutors map[model.StreamJob_Type]flinkService.Executor
}

func NewJobManagerService(ctx context.Context, db *gorm.DB, uClient utils.UdfClient, eClient utils.EngineClient,
	rClient utils.ResourceClient, logger *glog.Logger, zeppelinConfig zeppelin.ClientConfig,
	flinkConfig flink.ClientConfig) *JobManagerService {
	flinkBase := flinkService.NewBaseManager(ctx, db, logger, eClient, uClient, rClient, flinkConfig, zeppelinConfig)
	return &JobManagerService{
		ctx:            ctx,
		flinkExecutors: createFlinkExecutor(ctx, flinkBase),
	}
}

func (jm *JobManagerService) RunFlinkJob(ctx context.Context, jobInfo *request.JobInfo) (*response.JobInfo, error) {
	res := response.JobInfo{}
	executor := jm.flinkExecutors[jobInfo.Code.Type]

	result, err := executor.Run(ctx, jobInfo)
	if err != nil {
		return nil, err
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
	return &res, nil
}

func (jm *JobManagerService) CancelFlinkJob(ctx context.Context, jobType model.StreamJob_Type, jobId string, spaceId string, clusterId string) error {
	executor := jm.flinkExecutors[jobType]
	return executor.Cancel(ctx, jobId, spaceId, clusterId)
}

func (jm *JobManagerService) GetFlinkJob(ctx context.Context, jobType model.StreamJob_Type, jobId string,
	jobName string, spaceId string, clusterId string) (*response.JobInfo, error) {
	res := response.JobInfo{}
	executor := jm.flinkExecutors[jobType]
	job, err := executor.GetInfo(ctx, jobId, jobName, spaceId, clusterId)
	if err != nil {
		return nil, err
	}
	res.JobId = job.Jid
	return &res, nil
}

func (jm *JobManagerService) ValidateCode(jobType model.StreamJob_Type, code string) (*response.JobValidate, error) {
	res := response.JobValidate{}
	executor := jm.flinkExecutors[jobType]
	if flag, msg, err := executor.Validate(code); err != nil {
		return nil, err
	} else {
		res.Message = msg
		res.Flag = flag
		return &res, nil
	}
}

func createFlinkExecutor(ctx context.Context, bm *flinkService.BaseExecutor) map[model.StreamJob_Type]flinkService.Executor {
	executors := map[model.StreamJob_Type]flinkService.Executor{}
	executors[model.StreamJob_SQL] = flinkService.NewExecutor(ctx, model.StreamJob_SQL, bm)
	executors[model.StreamJob_Jar] = flinkService.NewExecutor(ctx, model.StreamJob_Jar, bm)
	executors[model.StreamJob_Scala] = flinkService.NewExecutor(ctx, model.StreamJob_Scala, bm)
	return executors
}
