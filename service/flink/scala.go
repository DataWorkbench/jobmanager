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
	bm  *BaseExecutor
	ctx context.Context
}

func NewScalaExecutor(bm *BaseExecutor, ctx context.Context) *ScalaExecutor {
	return &ScalaExecutor{
		bm:  bm,
		ctx: ctx,
	}
}

func (scalaExec *ScalaExecutor) Run(ctx context.Context, info *request.RunJob) (*zeppelin.ExecuteResult, error) {
	udfs, err := scalaExec.bm.getUDFs(ctx, info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	properties, err := scalaExec.bm.getGlobalProperties(ctx, info, udfs)
	if err != nil {
		return nil, err
	}
	session := zeppelin.NewZSessionWithProperties(scalaExec.bm.zeppelinConfig, FLINK, properties)
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
	jobProp["jobName"] = info.GetInstanceId()
	if info.GetArgs().GetParallelism() > 0 {
		jobProp["parallelism"] = strconv.FormatInt(int64(info.GetArgs().GetParallelism()), 10)
	}

	if result, err = session.SubmitWithProperties("", jobProp, info.GetCode().Scala.Code); err != nil {
		return nil, err
	}
	defer func() {
		if result != nil && (len(result.Results) > 0 || len(result.JobId) == 32) {
			if len(result.JobId) != 32 && (result.Status.IsRunning() || result.Status.IsPending()) {
				_ = session.Stop()
			} else {
				jobInfo := scalaExec.bm.TransResult(info.SpaceId, info.InstanceId, result)
				if err := scalaExec.bm.UpsertResult(ctx, jobInfo); err != nil {
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
				result.JobId = jobUrl[strings.LastIndex(jobUrl, "/")+1:]
			}
			return result, nil
		}
		time.Sleep(time.Second * 1)
	}
}

func (scalaExec *ScalaExecutor) GetInfo(ctx context.Context, instanceId string, spaceId string, clusterId string) (*flink.Job, error) {
	return scalaExec.bm.GetJobInfo(ctx, instanceId, spaceId, clusterId)
}

func (scalaExec *ScalaExecutor) Cancel(ctx context.Context, instanceId string, spaceId string, clusterId string) error {
	return scalaExec.bm.CancelJob(ctx, instanceId, spaceId, clusterId)
}

func (scalaExec *ScalaExecutor) Validate(jobCode *model.StreamJobCode) (bool, string, error) {
	return true, "", nil
}
