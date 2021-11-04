package server

import (
	"context"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/gproto/pkg/jobpb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"

	"github.com/DataWorkbench/jobmanager/executor"
)

type JobManagerServer struct {
	jobpb.UnimplementedJobmanagerServer
	executor *executor.JobmanagerExecutor
}

// NewJobManagerServer
func NewJobManagerServer(executor *executor.JobmanagerExecutor) *JobManagerServer {
	return &JobManagerServer{
		executor: executor,
	}
}

func (s *JobManagerServer) Run(ctx context.Context, req *request.JobInfo) (*response.JobState, error) {
	rep, err := s.executor.RunJob(ctx, req, constants.JobCommandRun)
	return &rep, err
}

func (s *JobManagerServer) Syntax(ctx context.Context, req *request.JobInfo) (*response.JobState, error) {
	rep, err := s.executor.RunJob(ctx, req, constants.JobCommandSyntax)
	return &rep, err
}

func (s *JobManagerServer) Preview(ctx context.Context, req *request.JobInfo) (*response.JobState, error) {
	rep, err := s.executor.RunJob(ctx, req, constants.JobCommandPreview)
	return &rep, err
}

func (s *JobManagerServer) CancelJob(ctx context.Context, req *request.JobCancel) (*model.EmptyStruct, error) {
	err := s.executor.CancelJob(ctx, req.GetJobId())
	return &model.EmptyStruct{}, err
}

func (s *JobManagerServer) CancelAllJob(ctx context.Context, req *request.DeleteWorkspaces) (*model.EmptyStruct, error) {
	err := s.executor.CancelAllJob(ctx, req.GetSpaceIds())
	return &model.EmptyStruct{}, err
}

func (s *JobManagerServer) GetState(ctx context.Context, req *request.JobGetState) (*response.JobState, error) {
	rep, err := s.executor.GetState(ctx, req.GetJobId())
	return &rep, err
}

func (s *JobManagerServer) NodeRelations(ctx context.Context, req *model.EmptyStruct) (*response.NodeRelations, error) {
	resp, err := s.executor.NodeRelations(ctx)
	return resp, err
}
