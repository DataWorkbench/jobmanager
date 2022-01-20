package service

import (
	"context"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/getcd"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/jobmanager/utils"

	"gorm.io/gorm"
)

type JobManagerService struct {
	ctx           context.Context
	flinkExecutor *FlinkExecutor
	etcd          *getcd.Client
}

func NewJobManagerService(ctx context.Context, db *gorm.DB, uClient utils.UdfClient, eClient utils.EngineClient,
	rClient utils.ResourceClient, fClient *flink.Client,
	zClient *zeppelin.Client, etcdClient *getcd.Client) *JobManagerService {
	return &JobManagerService{
		ctx:           ctx,
		flinkExecutor: NewFlinkExecutor(ctx, db, eClient, uClient, rClient, fClient, zClient),
		etcd:          etcdClient,
	}
}

func (jm *JobManagerService) InitFlinkJob(ctx context.Context, req *request.InitFlinkJob) (*response.InitFlinkJob, error) {
	res := response.InitFlinkJob{}
	noteId, paragraphId, err := jm.flinkExecutor.InitJob(ctx, req)
	if err != nil {
		return nil, err
	}
	res.NoteId = noteId
	res.ParagraphId = paragraphId
	return &res, nil
}

func (jm *JobManagerService) SubmitFlinkJob(ctx context.Context, req *request.SubmitFlinkJob) (*response.SubmitFlinkJob, error) {
	//mutex, err := getcd.NewMutex(ctx, jm.etcd, req.GetInstanceId())
	//if err != nil {
	//	return nil, err
	//}
	//if err = mutex.TryLock(ctx); err != nil {
	//	return nil, err
	//}
	//defer func() {
	//	_ = mutex.Unlock(ctx)
	//}()
	res := response.SubmitFlinkJob{}
	result, err := jm.flinkExecutor.SubmitJob(ctx, req.GetInstanceId(), req.GetNoteId(), req.GetParagraphId(), req.GetType())
	if err != nil {
		return nil, err
	}
	data, state := jm.flinkExecutor.transResult(result)
	res.Message = data
	res.State = state
	if result.JobId != "" && len(result.JobId) == 32 {
		res.FlinkId = result.JobId
	}
	return &res, nil
}

func (jm *JobManagerService) FreeFlinkJob(ctx context.Context, instanceId string, noteId string) error {
	return jm.flinkExecutor.Release(ctx, instanceId, noteId)
}

func (jm *JobManagerService) CancelFlinkJob(ctx context.Context, flinkId string, spaceId string, clusterId string) error {
	return jm.flinkExecutor.CancelJob(ctx, flinkId, spaceId, clusterId)
}

func (jm *JobManagerService) GetFlinkJob(ctx context.Context, flinkId string, spaceId string, clusterId string) (*response.GetFlinkJob, error) {
	res := response.GetFlinkJob{}
	job, err := jm.flinkExecutor.GetJobInfo(ctx, flinkId, spaceId, clusterId)
	if err != nil {
		return nil, err
	}
	switch job.State {
	case "FAILED":
		res.State = model.StreamJobInst_Failed
	case "FAILING", "INITIALIZING", "RESTARTING", "RECONCILING", "CANCELLING":
		res.State = model.StreamJobInst_Pending
	case "CREATED", "CANCELED", "SUSPENDED":
		res.State = model.StreamJobInst_Succeed
	case "RUNNING", "FINISHED":
		res.State = model.StreamJobInst_Running
	}
	if job.Exceptions != nil {
		res.Message = job.Exceptions.RootException
	}
	return &res, nil
}

func (jm *JobManagerService) ValidateFlinkCode(ctx context.Context, jobCode *request.ValidateFlinkJob) (*response.StreamJobCodeSyntax, error) {
	res := response.StreamJobCodeSyntax{}

	if flag, msg, err := jm.flinkExecutor.ValidateCode(ctx, jobCode); err != nil {
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
