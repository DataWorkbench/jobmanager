package flink

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type ScalaExecutor struct {
	*BaseExecutor
	ctx context.Context
}

func NewScalaExecutor(bm *BaseExecutor, ctx context.Context) *ScalaExecutor {
	return &ScalaExecutor{
		BaseExecutor: bm,
		ctx:          ctx,
	}
}

func (scalaExec *ScalaExecutor) Run(ctx context.Context, info *request.RunJob) (*zeppelin.ParagraphResult, error) {
	var result *zeppelin.ParagraphResult
	var noteId string
	defer func() {
		if result != nil && result.Status != zeppelin.RUNNING && len(noteId) > 0 {
			_ = scalaExec.zeppelinClient.DeleteNote(noteId)
		}
	}()
	result, err := scalaExec.preCheck(ctx, info.InstanceId)
	if err != nil {
		return nil, err
	} else if result != nil {
		return result, nil
	}

	udfs, err := scalaExec.getUDFs(ctx, info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	properties, err := scalaExec.getGlobalProperties(ctx, info, udfs)
	if err != nil {
		return nil, err
	}
	noteId, err = scalaExec.initNote("flink", info.GetInstanceId(), properties)
	if err != nil {
		return nil, err
	}
	result, err = scalaExec.registerUDF(noteId, udfs)
	if err != nil {
		return nil, err
	}

	jobProp := map[string]string{}
	if info.GetArgs().GetParallelism() > 0 {
		jobProp["parallelism"] = strconv.FormatInt(int64(info.GetArgs().GetParallelism()), 10)
	}
	if result, err = scalaExec.zeppelinClient.Submit("flink", "", noteId, info.GetCode().GetScala().GetCode()); err != nil {
		return result, err
	}
	if err = scalaExec.PreHandle(ctx, info.InstanceId, noteId, result.ParagraphId); err != nil {
		return nil, err
	}
	defer func() {
		scalaExec.PostHandle(ctx, info.InstanceId, noteId, result)
	}()
	for {
		if result, err = scalaExec.zeppelinClient.QueryParagraphResult(noteId, result.ParagraphId); err != nil {
			return result, err
		}
		if result.Status.IsFailed() {
			return result, err
		}
		if len(result.JobUrls) > 0 {
			jobUrl := result.JobUrls[0]
			if len(jobUrl)-1-strings.LastIndex(jobUrl, "/") == 32 {
				result.JobId = jobUrl[strings.LastIndex(jobUrl, "/")+1:]
			}
			return result, nil
		}
		time.Sleep(time.Second * 5)
	}
}

func (scalaExec *ScalaExecutor) GetInfo(ctx context.Context, instanceId string, spaceId string, clusterId string) (*flink.Job, error) {
	return scalaExec.getJobInfo(ctx, instanceId, spaceId, clusterId)
}

func (scalaExec *ScalaExecutor) Cancel(ctx context.Context, instanceId string, spaceId string, clusterId string) error {
	return scalaExec.cancelJob(ctx, instanceId, spaceId, clusterId)
}

func (scalaExec *ScalaExecutor) Release(ctx context.Context,instanceId string) error{
	return scalaExec.release(ctx,instanceId)
}

func (scalaExec *ScalaExecutor) Validate(jobCode *model.StreamJobCode) (bool, string, error) {
	return true, "", nil
}
