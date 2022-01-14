package server

import (
	"context"
	"github.com/DataWorkbench/gproto/pkg/jobpb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/jobmanager/service"
)

type JobManagerServer struct {
	jobpb.UnimplementedJobmanagerServer
	service *service.JobManagerService
}

func NewJobManagerServer(service *service.JobManagerService) *JobManagerServer {
	return &JobManagerServer{service: service}
}

func (s *JobManagerServer) FreeFlinkJob(ctx context.Context, req *request.FreeFlinkJob) (*model.EmptyStruct, error) {
	return &model.EmptyStruct{}, s.service.FreeFlinkJob(ctx, req.GetInstanceId())
}
func (s *JobManagerServer) InitFlinkJob(ctx context.Context, req *request.InitFlinkJob) (*response.InitFlinkJob, error) {
	return s.service.InitFlinkJob(ctx, req)
}
func (s *JobManagerServer) SubmitFlinkJob(ctx context.Context, req *request.SubmitFlinkJob) (*response.SubmitFlinkJob, error) {
	return s.service.SubmitFlinkJob(ctx, req)
}
func (s *JobManagerServer) GetFlinkJob(ctx context.Context, req *request.GetFlinkJob) (*response.GetFlinkJob, error) {
	return s.service.GetFlinkJob(ctx, req.GetFlinkId(), req.GetSpaceId(), req.GetClusterId())
}
func (s *JobManagerServer) CancelFlinkJob(ctx context.Context, req *request.CancelFlinkJob) (*model.EmptyStruct, error) {
	return &model.EmptyStruct{}, s.service.CancelFlinkJob(ctx, req.GetFlinkId(), req.GetSpaceId(), req.GetClusterId())
}
func (s *JobManagerServer) ValidateFlinkJob(ctx context.Context, req *request.ValidateFlinkJob) (*response.StreamJobCodeSyntax, error) {
	return s.service.ValidateFlinkCode(ctx, req)
}
