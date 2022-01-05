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

func (s *JobManagerServer) RunJob(ctx context.Context, req *request.RunJob) (*response.RunJob, error) {
	return s.service.RunFlinkJob(ctx, req)
}

func (s *JobManagerServer) GetJobInfo(ctx context.Context, req *request.GetJobInfo) (*response.GetJobInfo, error) {
	return s.service.GetFlinkJob(ctx, req.GetType(), req.GetInstanceId(), req.GetSpaceId(), req.GetClusterId())
}

func (s *JobManagerServer) CancelJob(ctx context.Context, req *request.CancelJob) (*model.EmptyStruct, error) {
	return &model.EmptyStruct{}, s.service.CancelFlinkJob(ctx, req.GetType(), req.GetInstanceId(), req.GetSpaceId(), req.GetClusterId())
}

func (s *JobManagerServer) ValidateJob(ctx context.Context, req *request.ValidateJob) (*response.StreamJobCodeSyntax, error) {
	return s.service.ValidateCode(req.Code)
}
