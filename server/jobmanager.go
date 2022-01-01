package server

import (
	"context"
	"github.com/DataWorkbench/gproto/pkg/jobpb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"github.com/DataWorkbench/jobmanager/executor"
)

type JobManagerServer struct {
	jobpb.UnimplementedJobmanagerServer
	service *executor.JobManagerService
}

func NewJobManagerServer(service *executor.JobManagerService) *JobManagerServer {
	return &JobManagerServer{service: service}
}

func (s *JobManagerServer) RunJob(ctx context.Context, req *request.JobInfo) (*response.JobInfo, error) {
	return s.service.RunJob(ctx, req)
}

func (s *JobManagerServer) GetJobInfo(ctx context.Context, req *request.JobMessage) (*response.JobInfo, error) {
	return s.service.GetJobInfo(ctx, req.GetType(), req.GetFlinkId(), req.GetJobId(), req.GetSpaceId(), req.GetSpaceId())
}

func (s *JobManagerServer) CancelJob(ctx context.Context, req *request.JobMessage) (*model.EmptyStruct, error) {
	return &model.EmptyStruct{}, s.service.CancelJob(ctx, req.GetType(), req.GetFlinkId(), req.GetSpaceId(), req.GetClusterId())
}
