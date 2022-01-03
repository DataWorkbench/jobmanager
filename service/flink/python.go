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

type PythonExecutor struct {
	bm  *BaseExecutor
	ctx context.Context
}

func NewPythonExecutor(bm *BaseExecutor, ctx context.Context) *ScalaExecutor {
	return &ScalaExecutor{
		bm:  bm,
		ctx: ctx,
	}
}

func (pyExec *PythonExecutor) Run(ctx context.Context, info *request.JobInfo) (*zeppelin.ExecuteResult, error) {
	udfs, err := pyExec.bm.getUDFs(ctx, info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	properties, err := pyExec.bm.getGlobalProperties(ctx, info, udfs)
	if err != nil {
		return nil, err
	}
	session := zeppelin.NewZSessionWithProperties(pyExec.bm.zeppelinConfig, FLINK, properties)
	if err = session.Start(); err != nil {
		return nil, err
	}
	var result *zeppelin.ExecuteResult
	for _, udf := range udfs {
		switch udf.udfType {
		case model.UDFInfo_Scala:
			result, err = session.Exec(udf.code)
			if err != nil {
				return nil, err
			}
		case model.UDFInfo_Python:
			result, err = session.Execute("ipyflink", udf.code)
			if err != nil {
				return nil, err
			}
		}
	}
	jobProp := map[string]string{}
	jobProp["jobName"] = info.GetJobId()
	if info.GetArgs().GetParallelism() > 0 {
		jobProp["parallelism"] = strconv.FormatInt(int64(info.GetArgs().GetParallelism()), 10)
	}

	if result, err = session.SubmitWithProperties("ipyflink", jobProp, info.GetCode().Scala.Code); err != nil {
		return result, err
	}
	defer func() {
		if result != nil && (len(result.Results) > 0 || len(result.JobUrls) > 0) {
			if (result.Status.IsRunning() || result.Status.IsPending()) &&
				result.JobUrls != nil && len(result.JobUrls) > 0 {
				_ = session.Stop()
			} else {
				jobInfo := pyExec.bm.TransResult(info.SpaceId, info.JobId, result)
				if err = pyExec.bm.UpsertResult(ctx, jobInfo); err != nil {
					_ = session.Stop()
				}
			}
		}
	}()
	for {
		if result, err = session.QueryStatement(result.StatementId); err != nil {
			return result, err
		}
		if result.Status.IsFailed() {
			return result, err
		}
		if len(result.JobUrls) > 0 {
			jobUrl := result.JobUrls[0]
			if len(jobUrl)-1-strings.LastIndex(jobUrl, "/") == 32 {
				jobId := jobUrl[strings.LastIndex(jobUrl, "/")+1:]
				urls := []string{jobId}
				result.JobUrls = urls
			}
			return result, nil
		}
		time.Sleep(time.Second * 1)
	}
}

func (pyExec *PythonExecutor) GetInfo(ctx context.Context, jobId string, jobName string, spaceId string, clusterId string) (*flink.Job, error) {
	return pyExec.bm.GetJobInfo(ctx, jobId, jobName, spaceId, clusterId)
}

func (pyExec *PythonExecutor) Cancel(ctx context.Context, jobId string, spaceId string, clusterId string) error {
	return pyExec.bm.CancelJob(ctx, jobId, spaceId, clusterId)
}

func (pyExec *PythonExecutor) Validate(code string) (bool, string, error) {
	return true, "", nil
}
