package server

import (
	"context"

	"github.com/DataWorkbench/gproto/xgo/service/pbsvcdeveloper"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
	"github.com/DataWorkbench/gproto/xgo/types/pbresponse"
	"github.com/DataWorkbench/jobmanager/service"
)

type JobManagerServer struct {
	pbsvcdeveloper.UnimplementedJobManageServer
	service *service.JobManagerService
}

func NewJobManagerServer(service *service.JobManagerService) *JobManagerServer {
	return &JobManagerServer{service: service}
}

func (s *JobManagerServer) FreeFlinkJob(ctx context.Context, req *pbrequest.FreeFlinkJob) (*pbmodel.EmptyStruct, error) {
	return &pbmodel.EmptyStruct{}, s.service.FreeFlinkJob(ctx, req.GetInstanceId(), req.GetNoteId())
}
func (s *JobManagerServer) InitFlinkJob(ctx context.Context, req *pbrequest.InitFlinkJob) (*pbresponse.InitFlinkJob, error) {
	return s.service.InitFlinkJob(ctx, req)
}
func (s *JobManagerServer) SubmitFlinkJob(ctx context.Context, req *pbrequest.SubmitFlinkJob) (*pbresponse.SubmitFlinkJob, error) {
	return s.service.SubmitFlinkJob(ctx, req)
}
func (s *JobManagerServer) GetFlinkJob(ctx context.Context, req *pbrequest.GetFlinkJob) (*pbresponse.GetFlinkJob, error) {
	return s.service.GetFlinkJob(ctx, req.GetFlinkId(), req.GetSpaceId(), req.GetClusterId())
}
func (s *JobManagerServer) CancelFlinkJob(ctx context.Context, req *pbrequest.CancelFlinkJob) (*pbmodel.EmptyStruct, error) {
	return &pbmodel.EmptyStruct{}, s.service.CancelFlinkJob(ctx, req.GetFlinkId(), req.GetSpaceId(), req.GetClusterId())
}
func (s *JobManagerServer) ValidateFlinkJob(ctx context.Context, req *pbrequest.ValidateFlinkJob) (*pbresponse.StreamJobCodeSyntax, error) {
	return s.service.ValidateFlinkCode(ctx, req)
}
