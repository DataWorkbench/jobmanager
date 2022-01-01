package flink

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type ScalaExecutor struct {
	bm     *BaseExecutor
	ctx    context.Context
	logger *glog.Logger
}

func NewScalaExecutor(bm *BaseExecutor, ctx context.Context, logger *glog.Logger) *ScalaExecutor {
	return &ScalaExecutor{
		bm:     bm,
		ctx:    ctx,
		logger: logger,
	}
}

func (scalaExec *ScalaExecutor) Run(ctx context.Context, info *request.JobInfo) (*zeppelin.ExecuteResult, error) {
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
	jobProp["jobName"] = info.GetJobId()
	if info.GetArgs().GetParallelism() > 0 {
		jobProp["parallelism"] = strconv.FormatInt(int64(info.GetArgs().GetParallelism()), 10)
	}
	if result, err = session.SubmitWithProperties("", jobProp, info.GetCode().Scala.Code); err != nil {
		return result, err
	}
	if result, err = session.WaitUntilRunning(result.StatementId); err != nil {
		return result, err
	}
	start := time.Now().Unix()
	for {
		if result, err = session.QueryStatement(result.StatementId); err != nil {
			return result, err
		}
		if result.Status.IsFailed() {
			var reason string
			if len(result.Results) > 0 {
				reason = result.Results[0].Data
				scalaExec.logger.Warn().Msg(reason)
			}
			return result, nil
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
		if time.Now().Unix()-start >= 30000 {
			return result, nil
		}
	}
}

func (scalaExec *ScalaExecutor) GetInfo(ctx context.Context, jobId string, jobName string, spaceId string, clusterId string) (*flink.Job, error) {
	return scalaExec.bm.GetJobInfo(ctx, jobId, jobName, spaceId, clusterId)
}

func (scalaExec *ScalaExecutor) Cancel(ctx context.Context, jobId string, spaceId string, clusterId string) error {
	return scalaExec.bm.CancelJob(ctx, jobId, spaceId, clusterId)
}
