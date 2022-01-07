package flink

import (
	"context"
	"github.com/DataWorkbench/common/qerror"
	"strconv"
	"strings"
	"time"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type PythonExecutor struct {
	*BaseExecutor
	ctx context.Context
}

func NewPythonExecutor(bm *BaseExecutor, ctx context.Context) *PythonExecutor {
	return &PythonExecutor{
		BaseExecutor: bm,
		ctx:          ctx,
	}
}

func (pyExec *PythonExecutor) Run(ctx context.Context, info *request.RunJob) (*zeppelin.ParagraphResult, error) {
	var result *zeppelin.ParagraphResult
	var noteId string
	var err error
	defer func() {
		if err == nil {
			if result != nil && result.Status != zeppelin.RUNNING && len(noteId) > 0 {
				if result.Status == zeppelin.ERROR && len(result.Results) > 0 {
					for _, re := range result.Results {
						if strings.EqualFold(re.Type, "TEXT") && strings.Contains(re.Data, "Caused by: java.net.ConnectException: Connection refused") {
							err = qerror.FlinkRestError
						}
					}
				}
				_ = pyExec.zeppelinClient.DeleteNote(noteId)
			} else if (result.Status == zeppelin.RUNNING || result.Status == zeppelin.FINISHED) &&
				len(result.JobId) != 32 && len(noteId) > 0 {
				result.Status = zeppelin.ABORT
				_ = pyExec.zeppelinClient.DeleteNote(noteId)
			}
		}
	}()

	result, err = pyExec.preCheck(ctx, info.InstanceId)
	if err != nil {
		return nil, err
	} else if result != nil {
		return result, nil
	}

	udfs, err := pyExec.getUDFs(ctx, info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	properties, err := pyExec.getGlobalProperties(ctx, info, udfs)
	if err != nil {
		return nil, err
	}
	noteId, err = pyExec.initNote("flink", info.GetInstanceId(), properties)
	if err != nil {
		return nil, err
	}
	result, err = pyExec.registerUDF(noteId, udfs)
	if err != nil {
		return nil, err
	}

	jobProp := map[string]string{}
	if info.GetArgs().GetParallelism() > 0 {
		jobProp["parallelism"] = strconv.FormatInt(int64(info.GetArgs().GetParallelism()), 10)
	}
	if result, err = pyExec.zeppelinClient.Submit("flink", "ipyflink", noteId, info.GetCode().GetPython().GetCode()); err != nil {
		return result, err
	}
	if err = pyExec.preHandle(ctx, info.InstanceId, noteId, result.ParagraphId); err != nil {
		return nil, err
	}
	defer func() {
		pyExec.postHandle(ctx, info.InstanceId, noteId, result)
	}()
	for {
		if result, err = pyExec.zeppelinClient.QueryParagraphResult(noteId, result.ParagraphId); err != nil {
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

func (pyExec *PythonExecutor) GetInfo(ctx context.Context, instanceId string, spaceId string, clusterId string) (*flink.Job, error) {
	return pyExec.getJobInfo(ctx, instanceId, spaceId, clusterId)
}

func (pyExec *PythonExecutor) Cancel(ctx context.Context, instanceId string, spaceId string, clusterId string) error {
	return pyExec.cancelJob(ctx, instanceId, spaceId, clusterId)
}

func (pyExec *PythonExecutor) Release(ctx context.Context,instanceId string) error{
	return pyExec.release(ctx,instanceId)
}

func (pyExec *PythonExecutor) Validate(jobCode *model.StreamJobCode) (bool, string, error) {
	return true, "", nil
}
