package executor

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type SqlManagerExecutor struct {
	bm             *BaseManagerExecutor
	zeppelinConfig zeppelin.ClientConfig
	ctx            context.Context
	logger         *glog.Logger
}

func NewSqlManagerExecutor(bm *BaseManagerExecutor, zeppelinConfig zeppelin.ClientConfig, ctx context.Context, logger *glog.Logger) *SqlManagerExecutor {
	return &SqlManagerExecutor{
		bm:             bm,
		zeppelinConfig: zeppelinConfig,
		ctx:            ctx,
		logger:         logger,
	}
}

func (sqlExec *SqlManagerExecutor) Run(ctx context.Context, info *request.JobInfo) (*zeppelin.ExecuteResult, error) {
	udfs, err := sqlExec.bm.getUDFs(ctx, info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	properties, err := sqlExec.bm.getGlobalProperties(ctx, info, udfs)
	if err != nil {
		return nil, err
	}
	session := zeppelin.NewZSessionWithProperties(sqlExec.zeppelinConfig, FLINK, properties)
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
	if strings.Contains(strings.ToLower(info.GetCode().Sql.Code), "insert") {
		jobProp["runAsOne"] = "true"
	}
	// TODO if execute with batch type ssql waitUntilFinished
	if result, err = session.SubmitWithProperties("ssql", jobProp, info.GetCode().Sql.Code); err != nil {
		return nil, err
	}
	if result, err = session.WaitUntilRunning(result.StatementId); err != nil {
		return nil, err
	}
	start := time.Now().Unix()
	for len(result.JobUrls) == 0 {
		result, err = session.QueryStatement(result.StatementId)
		if result.Status.IsFailed() {
			var reason string
			if len(result.Results) > 0 {
				reason = result.Results[0].Data
			}
			return nil, qerror.ZeppelinParagraphRunError.Format(reason)
		}
		if time.Now().Unix()-start >= 30000 {
			return result, nil
		}
	}
	if result.JobUrls != nil && len(result.JobUrls) > 0 &&
		strings.LastIndex(result.JobUrls[0], "/") > 0 &&
		strings.LastIndex(result.JobUrls[0], "/")+1 < len(result.JobUrls) {
		if jobId := result.JobUrls[0][strings.LastIndex(result.JobUrls[0], "/")+1:]; len(jobId) == 32 {
			result.JobUrls = append(result.JobUrls, jobId)
			return result, nil
		}
	}

	return result, nil
}

func (sqlExec *SqlManagerExecutor) GetInfo(ctx context.Context, jobId string, jobName string, spaceId string, clusterId string) (*flink.Job, error) {
	return sqlExec.bm.GetJobInfo(ctx, jobId, jobName, spaceId, clusterId)
}

func (sqlExec *SqlManagerExecutor) Cancel(ctx context.Context, jobId string, spaceId string, clusterId string) error {
	return sqlExec.bm.CancelJob(ctx, jobId, spaceId, clusterId)
}
