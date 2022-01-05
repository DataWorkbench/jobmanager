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
	*BaseExecutor
	ctx context.Context
}

func NewPythonExecutor(bm *BaseExecutor, ctx context.Context) *PythonExecutor {
	return &PythonExecutor{
		BaseExecutor: bm,
		ctx:          ctx,
	}
}

func (pyExec *PythonExecutor) Run(ctx context.Context, info *request.RunJob) (*zeppelin.ExecuteResult, error) {
	udfs, err := pyExec.getUDFs(ctx, info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	properties, err := pyExec.getGlobalProperties(ctx, info, udfs)
	if err != nil {
		return nil, err
	}
	session := zeppelin.NewZSessionWithProperties(pyExec.zeppelinConfig, FLINK, properties)
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
	if info.GetArgs().GetParallelism() > 0 {
		jobProp["parallelism"] = strconv.FormatInt(int64(info.GetArgs().GetParallelism()), 10)
	}

	if result, err = session.SubmitWithProperties("ipyflink", jobProp, info.GetCode().Scala.Code); err != nil {
		return result, err
	}

	defer func() {
		pyExec.HandleResults(ctx, info.SpaceId, info.InstanceId, result, session)
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
				result.JobId = jobUrl[strings.LastIndex(jobUrl, "/")+1:]
			}
			return result, nil
		}
		time.Sleep(time.Second * 1)
	}
}

func (pyExec *PythonExecutor) GetInfo(ctx context.Context, instanceId string, spaceId string, clusterId string) (*flink.Job, error) {
	return pyExec.GetJobInfo(ctx, instanceId, spaceId, clusterId)
}

func (pyExec *PythonExecutor) Cancel(ctx context.Context, instanceId string, spaceId string, clusterId string) error {
	return pyExec.CancelJob(ctx, instanceId, spaceId, clusterId)
}

func (pyExec *PythonExecutor) Validate(jobCode *model.StreamJobCode) (bool, string, error) {
	return true, "", nil
}
