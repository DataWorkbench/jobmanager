package executor

import (
	"context"
	"strconv"
	"strings"
	"time"

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
	udfs, err := sqlExec.bm.getUDFs(info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	properties, err := sqlExec.bm.getGlobalProperties(info, udfs)
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
		if time.Now().Unix()-start >= 3000 {
			return result, nil
		}
	}
	for _, url := range result.JobUrls {
		if url != "" && len(url) > 0 && strings.Index(url, "/") > 0 {
			if jobId := url[strings.LastIndex(url, "/")+1:]; len(jobId) == 32 {
				result.JobUrls = append(result.JobUrls, jobId)
			}
		}
	}
	return result, nil
}
