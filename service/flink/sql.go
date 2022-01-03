package flink

import (
	"context"
	"encoding/base64"
	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"strconv"
	"strings"
	"time"
)

type SqlExecutor struct {
	bm  *BaseExecutor
	ctx context.Context
}

func NewSqlExecutor(ctx context.Context, bm *BaseExecutor) *SqlExecutor {
	return &SqlExecutor{
		bm:  bm,
		ctx: ctx,
	}
}

func (sqlExec *SqlExecutor) Run(ctx context.Context, info *request.JobInfo) (*zeppelin.ExecuteResult, error) {
	udfs, err := sqlExec.bm.getUDFs(ctx, info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	properties, err := sqlExec.bm.getGlobalProperties(ctx, info, udfs)
	if err != nil {
		return nil, err
	}
	session := zeppelin.NewZSessionWithProperties(sqlExec.bm.zeppelinConfig, FLINK, properties)
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
	if result, err = session.SubmitWithProperties("ssql", jobProp, info.GetCode().Sql.Code); err != nil {
		return result, err
	}
	// TODO if execute with batch type ssql waitUntilFinished
	defer func() {
		if result != nil && (len(result.Results) > 0 || len(result.JobUrls) > 0) {
			if (result.Status.IsRunning() || result.Status.IsPending()) &&
				result.JobUrls != nil && len(result.JobUrls) > 0 {
				_ = session.Stop()
			} else {
				jobInfo := sqlExec.bm.TransResult(info.SpaceId, info.JobId, result)
				if err = sqlExec.bm.UpsertResult(ctx, jobInfo); err != nil {
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

func (sqlExec *SqlExecutor) GetInfo(ctx context.Context, jobId string, jobName string, spaceId string, clusterId string) (*flink.Job, error) {
	return sqlExec.bm.GetJobInfo(ctx, jobId, jobName, spaceId, clusterId)
}

func (sqlExec *SqlExecutor) Cancel(ctx context.Context, jobId string, spaceId string, clusterId string) error {
	return sqlExec.bm.CancelJob(ctx, jobId, spaceId, clusterId)
}

func (sqlExec *SqlExecutor) Validate(code string) (bool, string, error) {
	builder := strings.Builder{}
	builder.WriteString("java -jar /Users/apple/develop/java/sql-vadilator/target/sql-validator.jar ")
	builder.WriteString(base64.StdEncoding.EncodeToString([]byte(code)))
	session := zeppelin.NewZSession(sqlExec.bm.zeppelinConfig, "sh")
	if err := session.Start(); err != nil {
		return false, "", err
	}
	if result, err := session.Exec(builder.String()); err != nil {
		return false, "", err
	} else if result.Results != nil && len(result.Results) > 0 {
		return false, result.Results[0].Data, nil
	}
	return true, "", nil
}
