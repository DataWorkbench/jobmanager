package server

import (
	"context"

	"github.com/DataWorkbench/gproto/pkg/jobpb"

	"github.com/DataWorkbench/jobmanager/executor"
)

type JobManagerServer struct {
	jobpb.UnimplementedJobmanagerServer
	executor   *executor.JobmanagerExecutor
	emptyReply *jobpb.EmptyReply
}

// NewJobManagerServer
func NewJobManagerServer(executor *executor.JobmanagerExecutor) *JobManagerServer {
	return &JobManagerServer{
		executor:   executor,
		emptyReply: &jobpb.EmptyReply{},
	}
}

func (s *JobManagerServer) RunJob(ctx context.Context, req *jobpb.RunJobRequest) (*jobpb.JobReply, error) {
	rep, err := s.executor.RunJob(ctx, req.GetID(), req.GetWorkspaceID(), req.GetNodeType(), req.GetDepends(), req.GetMainRun())
	return &rep, err
}

func (s *JobManagerServer) CancelJob(ctx context.Context, req *jobpb.CancelJobRequest) (*jobpb.EmptyReply, error) {
	err := s.executor.CancelJob(ctx, req.GetID())
	return s.emptyReply, err
}

func (s *JobManagerServer) GetJobStatus(ctx context.Context, req *jobpb.GetJobStatusRequest) (*jobpb.JobReply, error) {
	rep, err := s.executor.GetJobStatus(ctx, req.GetID())
	return &rep, err
}
