package flink

import (
	"context"
	"encoding/base64"
	"fmt"
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
		//TODO 如果 notebook已经被创建
		if noteId != "" && len(noteId) > 0 {
			// TODO 如果没有异常
			if err == nil {
				// TODO 判断结果 不是Running，删除notebook
				if result != nil && !result.Status.IsRunning() {
					_ = sqlExec.zeppelinClient.DeleteNote(noteId)
				}
				//TODO 如果有异常 直接删除notebook
			} else {
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
	sqlExec.logger.Info().Msg(fmt.Sprintf("flink job properties is %s", properties)).Fire()
	noteId, err = sqlExec.initNote("flink", info.GetInstanceId(), properties)
	if err != nil {
		return nil, err
	}
	sqlExec.logger.Info().Msg("flink job init success").Fire()
	result, err = sqlExec.registerUDF(noteId, udfs)
	if err != nil {
		return nil, err
	}

	jobProp := map[string]string{}
	if info.GetArgs().GetParallelism() > 0 {
		jobProp["parallelism"] = strconv.FormatInt(int64(info.GetArgs().GetParallelism()), 10)
	}
	if strings.Contains(strings.ToLower(info.GetCode().GetSql().GetCode()), "insert") {
		jobProp["runAsOne"] = "true"
	} else if strings.Contains(strings.ToLower(info.GetCode().GetSql().GetCode()), "select") {
		jobProp["type"] = "update"
	} else {
		sqlExec.logger.Info().Msg("this sql does not has dml command.")
		//TODO 这种没有dml的sql 也会成功但是没有jobId，这里按照Finished 处理
		result = &zeppelin.ParagraphResult{
			NoteId:      noteId,
			ParagraphId: "",
			Status:      zeppelin.FINISHED,
			Progress:    0,
			Results:     nil,
			JobUrls:     nil,
			JobId:       "",
		}
		return result, nil
	}

	sqlExec.logger.Info().Msg("flink job submit start.").Fire()
	if result, err = sqlExec.zeppelinClient.SubmitWithProperties("flink", "ssql", noteId, info.GetCode().GetSql().GetCode(), jobProp); err != nil {
		return result, err
	}
	sqlExec.logger.Info().Msg(fmt.Sprintf("flink job submit finish, tmp status is %s,result is %s.", result.Status, result.Results)).Fire()
	//TODO 异步提交后立马记录一下当前的状态，notebook id，paragraph id
	if err = sqlExec.preHandle(ctx, info.InstanceId, noteId, result.ParagraphId); err != nil {
		return nil, err
	}
	sqlExec.logger.Info().Msg(fmt.Sprintf("insert pre submit success, instance_id is %s ,note_id is %s ,paragraph_id is %s", info.InstanceId, noteId, result.ParagraphId)).Fire()

	defer func() {
		if err == nil {
			if result != nil && len(noteId) > 0 {
				// TODO 如果结束状态不是Running，去结果找是否是因为连接Flink超时导致的异常，如果只设置err为 FlinkRestError，返回
				if !result.Status.IsRunning() {
					for _, re := range result.Results {
						if strings.EqualFold(re.Type, "TEXT") && strings.Contains(re.Data, "Caused by: java.net.ConnectException: Connection refused") {
							sqlExec.logger.Error().Msg(fmt.Sprintf("flink cluster rest time out,cluster id is %s,job instanceId is %s",
								info.GetArgs().ClusterId, info.InstanceId))
							err = qerror.FlinkRestError
							return
						}
					}
					//TODO 如果结果状态为Running，但是JobId长度不符合
				} else if len(result.JobId) != 32 {
					result.Status = zeppelin.ERROR
				}
			}
			// TODO 如果没有异常，则结果写如数据库
			sqlExec.postHandle(ctx, info.InstanceId, noteId, result)
		}
	}()
	for {
		if result, err = sqlExec.zeppelinClient.QueryParagraphResult(noteId, result.ParagraphId); err != nil {
			return nil, err
		}
		sqlExec.logger.Info().Msg(fmt.Sprintf("query result for instance %s until submit success, status %s, result %s", info.InstanceId, result.Status, result.Results)).Fire()
		if result.Status.IsFailed() {
			return result, nil
		}
		if len(result.JobUrls) > 0 {
			jobUrl := result.JobUrls[0]
			if len(jobUrl)-1-strings.LastIndex(jobUrl, "/") == 32 {
				result.JobId = jobUrl[strings.LastIndex(jobUrl, "/")+1:]
				sqlExec.logger.Info().Msg(fmt.Sprintf("fetch flink job id success, job id is %s", result.JobId)).Fire()
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
	if result, err := sqlExec.zeppelinClient.Execute("sh", "", noteId, builder.String()); err != nil {
		return false, "", err
	} else if result.Results != nil && len(result.Results) > 0 {
		if strings.Contains(result.Results[0].Data, "sql") {
			return false, "Invalid sql statements, please check you code.", nil
		}
		return false, result.Results[0].Data, nil
	}
	return true, "", nil
}
