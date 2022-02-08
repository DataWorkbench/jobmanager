package service

import (
	"context"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/getcd"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
	"github.com/DataWorkbench/gproto/xgo/types/pbresponse"
	"github.com/DataWorkbench/jobmanager/utils"

	"gorm.io/gorm"
)

type JobManagerService struct {
	ctx           context.Context
	flinkExecutor *FlinkExecutor
	etcd          *getcd.Client
}

func NewJobManagerService(ctx context.Context, db *gorm.DB, uClient utils.UdfClient, eClient utils.ClusterManagerClient,
	rClient utils.ResourceClient, fClient *flink.Client,
	zClient *zeppelin.Client, etcdClient *getcd.Client) *JobManagerService {
	return &JobManagerService{
		ctx:           ctx,
		flinkExecutor: NewFlinkExecutor(ctx, db, eClient, uClient, rClient, fClient, zClient),
		etcd:          etcdClient,
	}
}

func (jm *JobManagerService) InitFlinkJob(ctx context.Context, req *pbrequest.InitFlinkJob) (*pbresponse.InitFlinkJob, error) {
	res := pbresponse.InitFlinkJob{}
	noteId, paragraphId, err := jm.flinkExecutor.InitJob(ctx, req)
	if err != nil {
		return nil, err
	}
	res.NoteId = noteId
	res.ParagraphId = paragraphId
	return &res, nil
}

func (jm *JobManagerService) SubmitFlinkJob(ctx context.Context, req *pbrequest.SubmitFlinkJob) (*pbresponse.SubmitFlinkJob, error) {
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
	res := pbresponse.SubmitFlinkJob{}
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

func (jm *JobManagerService) GetFlinkJob(ctx context.Context, flinkId string, spaceId string, clusterId string) (*pbresponse.GetFlinkJob, error) {
	res := pbresponse.GetFlinkJob{}
	job, err := jm.flinkExecutor.GetJobInfo(ctx, flinkId, spaceId, clusterId)
	if err != nil {
		return nil, err
	}
	switch job.State {
	case "FAILED":
		res.State = pbmodel.StreamInstance_Failed
	case "FAILING", "INITIALIZING", "RESTARTING", "RECONCILING", "CANCELLING":
		res.State = pbmodel.StreamInstance_Pending
	case "CREATED", "CANCELED", "SUSPENDED", "FINISHED":
		res.State = pbmodel.StreamInstance_Succeed
	case "RUNNING":
		res.State = pbmodel.StreamInstance_Running
	}
	if job.Exceptions != nil {
		res.Message = job.Exceptions.RootException
	}
	return &res, nil
}

func (jm *JobManagerService) ValidateFlinkCode(ctx context.Context, jobCode *pbrequest.ValidateFlinkJob) (*pbresponse.StreamJobCodeSyntax, error) {
	res := pbresponse.StreamJobCodeSyntax{}

	if flag, msg, err := jm.flinkExecutor.ValidateCode(ctx, jobCode); err != nil {
		return nil, err
	} else {
		if flag {
			res.Result = pbresponse.StreamJobCodeSyntax_Correct
		} else {
			res.Result = pbresponse.StreamJobCodeSyntax_Incorrect
			res.Message = msg
		}
		return &res, nil
	}
}
