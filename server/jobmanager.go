package server

import (
	"context"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/gproto/pkg/jobpb"
	"github.com/DataWorkbench/gproto/pkg/model"

	"github.com/DataWorkbench/jobmanager/executor"
)

type JobManagerServer struct {
	jobpb.UnimplementedJobmanagerServer
	executor   *executor.JobmanagerExecutor
	emptyReply *model.EmptyStruct
}

// NewJobManagerServer
func NewJobManagerServer(executor *executor.JobmanagerExecutor) *JobManagerServer {
	return &JobManagerServer{
		executor:   executor,
		emptyReply: &model.EmptyStruct{},
	}
}

func (s *JobManagerServer) Run(ctx context.Context, req *jobpb.RunJobRequest) (*jobpb.JobReply, error) {
	rep, err := s.executor.RunJob(ctx, req.GetID(), req.GetSpaceID(), req.GetEngineID(), req.GetEngineType(), constants.RunCommand, req.GetJobInfo())
	return &rep, err
}

func (s *JobManagerServer) Syntax(ctx context.Context, req *jobpb.RunJobRequest) (*jobpb.JobReply, error) {
	rep, err := s.executor.RunJob(ctx, req.GetID(), req.GetSpaceID(), req.GetEngineID(), req.GetEngineType(), constants.SyntaxCheckCommand, req.GetJobInfo())
	return &rep, err
}
func (s *JobManagerServer) Preview(ctx context.Context, req *jobpb.RunJobRequest) (*jobpb.JobReply, error) {
	rep, err := s.executor.RunJob(ctx, req.GetID(), req.GetSpaceID(), req.GetEngineID(), req.GetEngineType(), constants.PreviewCommand, req.GetJobInfo())
	return &rep, err
}
func (s *JobManagerServer) Explain(ctx context.Context, req *jobpb.RunJobRequest) (*jobpb.JobReply, error) {
	rep, err := s.executor.RunJob(ctx, req.GetID(), req.GetSpaceID(), req.GetEngineID(), req.GetEngineType(), constants.ExplainCommand, req.GetJobInfo())
	return &rep, err
}

func (s *JobManagerServer) CancelJob(ctx context.Context, req *jobpb.CancelJobRequest) (*model.EmptyStruct, error) {
	err := s.executor.CancelJob(ctx, req.GetID())
	return s.emptyReply, err
}

func (s *JobManagerServer) GetJobState(ctx context.Context, req *jobpb.GetJobStateRequest) (*jobpb.JobReply, error) {
	rep, err := s.executor.GetJobState(ctx, req.GetID())
	return &rep, err
}

func (s *JobManagerServer) CancelAllJob(ctx context.Context, req *jobpb.CancelAllJobRequest) (*model.EmptyStruct, error) {
	err := s.executor.CancelAllJob(ctx, req.GetSpaceID())
	return s.emptyReply, err
}
