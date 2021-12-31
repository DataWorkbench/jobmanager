package executor

import (
	"context"
	"strings"

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

func NewJobManagerService(ctx context.Context, uClient utils.UdfClient, eClient utils.EngineClient,
	rClient utils.ResourceClient, logger *glog.Logger, config zeppelin.ClientConfig) *JobManagerService {
	return &JobManagerService{
		ctx:            ctx,
		logger:         logger,
		zeppelinConfig: config,
		bm:             NewBaseManager(eClient, uClient, rClient),
	}
}

func (jm *JobManagerService) RunJob(ctx context.Context, jobInfo *request.JobInfo) (res *response.JobInfo, err error) {
	var executor JobExecutor
	switch jobInfo.Code.Type {
	case model.StreamJob_SQL:
		executor = NewSqlManagerExecutor(jm.bm, jm.zeppelinConfig, ctx, jm.logger)
	case model.StreamJob_Jar:
		executor = NewJarManagerExecutor(jm.bm, jm.zeppelinConfig, ctx, jm.logger)
	case model.StreamJob_Python:
	case model.StreamJob_Scala:
		executor = NewScalaManagerExecutor(jm.bm, jm.zeppelinConfig, ctx, jm.logger)
	default:
	}
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
