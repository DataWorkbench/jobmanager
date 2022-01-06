package flink

import (
	"context"
	"encoding/base64"
	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/google/uuid"
	"strconv"
	"strings"
	"time"
)

type SqlExecutor struct {
	*BaseExecutor
	ctx context.Context
}

func NewSqlExecutor(ctx context.Context, bm *BaseExecutor) *SqlExecutor {
	return &SqlExecutor{
		BaseExecutor: bm,
		ctx:          ctx,
	}
}

func (sqlExec *SqlExecutor) Run(ctx context.Context, info *request.RunJob) (*zeppelin.ParagraphResult, error) {
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
				_ = sqlExec.zeppelinClient.DeleteNote(noteId)
			} else if (result.Status == zeppelin.RUNNING || result.Status == zeppelin.FINISHED) &&
				len(result.JobId) != 32 && len(noteId) > 0 {
				result.Status = zeppelin.ABORT
				_ = sqlExec.zeppelinClient.DeleteNote(noteId)
			}
		}
	}()

	// TODO 无论怎样都要加锁，能保证这个数据库查到的状态是上次调用结束后的状态
	result, err = sqlExec.preCheck(ctx, info.InstanceId)
	if err != nil {
		return nil, err
	} else if result != nil {
		return result, nil
	}

	udfs, err := sqlExec.getUDFs(ctx, info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	properties, err := sqlExec.getGlobalProperties(ctx, info, udfs)
	if err != nil {
		return nil, err
	}
	noteId, err = sqlExec.initNote("flink", info.GetInstanceId(), properties)
	if err != nil {
		return nil, err
	}
	result, err = sqlExec.registerUDF(noteId, udfs)
	if err != nil {
		return nil, err
	}

	jobProp := map[string]string{}
	if info.GetArgs().GetParallelism() > 0 {
		jobProp["parallelism"] = strconv.FormatInt(int64(info.GetArgs().GetParallelism()), 10)
	}
	if strings.Contains(strings.ToLower(info.GetCode().Sql.Code), "insert") {
		jobProp["runAsOne"] = "true"
	}
	if result, err = sqlExec.zeppelinClient.Submit("flink", "ssql", noteId, info.GetCode().GetSql().GetCode()); err != nil {
		return result, err
	}
	if err = sqlExec.PreHandle(ctx, info.InstanceId, noteId, result.ParagraphId); err != nil {
		return nil, err
	}
	defer func() {
		sqlExec.PostHandle(ctx, info.InstanceId, noteId, result)
	}()
	for {
		if result, err = sqlExec.zeppelinClient.QueryParagraphResult(noteId, result.ParagraphId); err != nil {
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

func (sqlExec *SqlExecutor) GetInfo(ctx context.Context, instanceId string, spaceId string, clusterId string) (*flink.Job, error) {
	return sqlExec.getJobInfo(ctx, instanceId, spaceId, clusterId)
}

func (sqlExec *SqlExecutor) Cancel(ctx context.Context, instanceId string, spaceId string, clusterId string) error {
	return sqlExec.cancelJob(ctx, instanceId, spaceId, clusterId)
}

func (sqlExec *SqlExecutor) Release(ctx context.Context, instanceId string) error {
	return sqlExec.release(ctx, instanceId)
}

func (sqlExec *SqlExecutor) Validate(jobCode *model.StreamJobCode) (bool, string, error) {
	builder := strings.Builder{}
	builder.WriteString("java -jar /zeppelin/flink/depends/sql-validator.jar ")
	//builder.WriteString("java -jar /Users/apple/develop/java/sql-vadilator/target/sql-validator.jar ")
	builder.WriteString(base64.StdEncoding.EncodeToString([]byte(jobCode.Sql.Code)))
	random, err := uuid.NewRandom()
	if err != nil {
		return false, "", err
	}
	noteName := random.String()
	noteId, err := sqlExec.zeppelinClient.CreateNote(noteName)
	defer func() {
		if len(noteId) > 0 {
			_ = sqlExec.zeppelinClient.DeleteNote(noteId)
		}
	}()
	if err != nil {
		return false, "", err
	}
	if result, err := sqlExec.zeppelinClient.Submit("sh", "", noteId, builder.String()); err != nil {
		return false, "", err
	} else if result.Results != nil && len(result.Results) > 0 {
		return false, result.Results[0].Data, nil
	}
	return true, "", nil
}
