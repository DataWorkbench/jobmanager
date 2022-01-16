package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/jobmanager/utils"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	FLINK      = "flink"
	SHELL      = "sh"
	Quote      = "$qc$"
	UDFQuote   = Quote + "_udf_name_" + Quote
	JobManager = "job_manager"
)

type FlinkExecutor struct {
	ctx            context.Context
	db             *gorm.DB
	engineClient   utils.EngineClient
	udfClient      utils.UdfClient
	resourceClient utils.ResourceClient
	flinkClient    *flink.Client
	zeppelinClient *zeppelin.Client
}

type Udf struct {
	udfType model.UDFInfo_Language
	code    string
}

func NewFlinkExecutor(ctx context.Context, db *gorm.DB, engineClient utils.EngineClient, udfClient utils.UdfClient, resourceClient utils.ResourceClient,
	flinkClient *flink.Client, zeppelinClient *zeppelin.Client) *FlinkExecutor {
	return &FlinkExecutor{
		ctx:            ctx,
		db:             db,
		engineClient:   engineClient,
		udfClient:      udfClient,
		resourceClient: resourceClient,
		flinkClient:    flinkClient,
		zeppelinClient: zeppelinClient,
	}
}

func (exec *FlinkExecutor) InitJob(ctx context.Context, req *request.InitFlinkJob) (string, string, error) {
	var (
		noteId      string
		paragraphId string
		result      *zeppelin.ParagraphResult
		err         error
		udfs        []*Udf
	)
	logger := glog.FromContext(ctx)
	defer func() {
		if err != nil {
			logger.Error().Msg(err.Error()).Fire()
			//if noteId != "" && len(noteId) > 0 {
			//	_ = exec.zeppelinClient.DeleteNote(noteId)
			//}
		} else {
			if result != nil && !result.Status.IsFinished() {
				var data string
				if result.Results != nil && len(result.Results) > 0 {
					for _, re := range result.Results {
						if strings.EqualFold("TEXT", re.Type) {
							data = re.Data
							break
						}
					}
				}
				logger.Error().Msg(fmt.Sprintf("connect flink cluster failed,reason is %s", data)).Fire()
				err = qerror.ZeppelinInitFailed
			}
			if noteId == "" || len(noteId) <= 0 || paragraphId == "" || len(paragraphId) <= 0 {
				logger.Error().Msg("init note id and paragraph id failed").Fire()
				err = qerror.ZeppelinInitFailed
			}
		}
	}()
	if udfs, err = exec.getUDFs(ctx, req.GetArgs().GetUdfs()); err != nil {
		return "", "", err
	}
	switch req.GetCode().GetType() {
	case model.StreamJob_SQL:
		if noteId, paragraphId, result, err = exec.initSql(ctx, req, udfs, logger); err != nil {
			return "", "", err
		}
		return noteId, paragraphId, nil
	case model.StreamJob_Jar:
		if noteId, paragraphId, result, err = exec.initJar(ctx, req, udfs, logger); err != nil {
			return "", "", err
		}
		return noteId, paragraphId, nil
	}
	logger.Error().Msg(fmt.Sprintf("job type not match,only support jar and sql,current type is %s", req.GetCode().GetType()))
	return "", "", nil
}

func (exec *FlinkExecutor) SubmitJob(ctx context.Context, instanceId string, noteId string, paragraphId string, jobType model.StreamJob_Type) (*zeppelin.ParagraphResult, error) {
	logger := glog.FromContext(ctx)
	result, err := exec.preCheck(ctx, instanceId)
	if err != nil {
		return nil, err
	} else if result != nil {
		return result, nil
	}

	switch jobType {
	case model.StreamJob_SQL:
		return exec.runSql(ctx, instanceId, noteId, paragraphId, logger)
	case model.StreamJob_Jar:
		return exec.runJar(ctx, instanceId, noteId, paragraphId, logger)
	}
	logger.Error().Msg(fmt.Sprintf("job type not match,only support jar and sql,current type is %s", jobType.Type()))
	return nil, nil
}

func (exec *FlinkExecutor) GetJobInfo(ctx context.Context, flinkId string, spaceId string, clusterId string) (*flink.Job, error) {
	var (
		flinkUrl   string
		err        error
		logger     = glog.FromContext(ctx)
		exceptions *flink.JobExceptions
	)
	if strings.EqualFold(os.Getenv("LOCAL"), "TRUE") {
		flinkUrl = "127.0.0.1:8081"
	} else {
		if flinkUrl, _, err = exec.engineClient.GetEngineInfo(ctx, spaceId, clusterId); err != nil {
			logger.Info().Msg(fmt.Sprintf("flink url is %s", flinkUrl)).Fire()
			return nil, err
		}
	}
	job, err := exec.flinkClient.GetInfo(ctx, flinkUrl, flinkId)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(job.State, "FAILED") {
		if exceptions, err = exec.flinkClient.GetExceptions(ctx, flinkUrl, flinkId); err != nil {
			return nil, err
		}
		job.Exceptions = exceptions
	}
	return job, nil
}

func (exec *FlinkExecutor) Release(ctx context.Context, instanceId string, noteId string) error {
	var err error
	logger := glog.FromContext(ctx)
	if err = exec.zeppelinClient.DeleteNote(ctx, noteId); err != nil {
		logger.Warn().Msg(fmt.Sprintf("delete note failed,noteid is %s,reason is %s", noteId, err.Error())).Fire()
	}
	return exec.deleteResult(ctx, instanceId)
}

func (exec *FlinkExecutor) CancelJob(ctx context.Context, flinkId string, spaceId string, clusterId string) error {
	var (
		flinkUrl string
		err      error
	)
	if strings.EqualFold(os.Getenv("LOCAL"), "TRUE") {
		flinkUrl = "127.0.0.1:8081"
	} else {
		if flinkUrl, _, err = exec.getFlinkMessage(ctx, spaceId, clusterId); err != nil {
			return err
		}
	}
	return exec.flinkClient.CancelJob(ctx, flinkUrl, flinkId)
}

func (exec *FlinkExecutor) ValidateCode(ctx context.Context, jobCode *request.ValidateFlinkJob) (bool, string, error) {
	switch jobCode.GetCode().GetType() {
	case model.StreamJob_SQL:
		builder := strings.Builder{}
		builder.WriteString("java -jar /zeppelin/flink/depends/sql-validator.jar ")
		//builder.WriteString("java -jar /Users/apple/develop/java/sql-vadilator/target/sql-validator.jar ")
		builder.WriteString(base64.StdEncoding.EncodeToString([]byte(jobCode.GetCode().GetSql().GetCode())))
		random, err := uuid.NewRandom()
		if err != nil {
			return false, "", err
		}
		noteName := random.String()
		noteId, err := exec.zeppelinClient.CreateNote(ctx, noteName)
		defer func() {
			if len(noteId) > 0 {
				_ = exec.zeppelinClient.DeleteNote(ctx, noteId)
			}
		}()
		if err != nil {
			return false, "", err
		}
		if result, err := exec.zeppelinClient.Execute(ctx, "sh", "", noteId, builder.String()); err != nil {
			return false, "", err
		} else if result.Results != nil && len(result.Results) > 0 {
			if strings.Contains(result.Results[0].Data, "Non-query expression") {
				return false, "Invalid sql statements, please check you code.", nil
			}
			return false, result.Results[0].Data, nil
		}
		return true, "", nil
	default:
		return true, "", nil
	}
}

func (exec *FlinkExecutor) initSql(ctx context.Context, req *request.InitFlinkJob, udfs []*Udf, logger *glog.Logger) (string, string, *zeppelin.ParagraphResult, error) {
	properties, err := exec.getGlobalProperties(ctx, req, udfs)
	if err != nil {
		return "", "", nil, err
	}
	noteId, err := exec.initNote(ctx, FLINK, req.GetInstanceId(), properties)
	if err != nil {
		return "", "", nil, err
	}
	result, err := exec.registerUDF(ctx, noteId, udfs)
	if err != nil {
		return "", "", nil, err
	}
	jobProp := map[string]string{}
	if req.GetArgs().GetParallelism() > 0 {
		jobProp["parallelism"] = strconv.FormatInt(int64(req.GetArgs().GetParallelism()), 10)
	}
	if strings.Contains(strings.ToLower(req.GetCode().GetSql().GetCode()), "insert") {
		for _, sql := range strings.Split(req.GetCode().GetSql().GetCode(), "\n") {
			if !strings.HasPrefix(sql, "--") && strings.Contains(strings.ToLower(sql), "insert") {
				jobProp["runAsOne"] = "true"
				break
			}
		}
	}
	if strings.Contains(strings.ToLower(req.GetCode().GetSql().GetCode()), "select") {
		jobProp["type"] = "update"
	}
	builder := strings.Builder{}
	builder.WriteString("%" + FLINK + ".ssql")
	if len(properties) > 0 {
		builder.WriteString("(")
		var propertyStr []string
		for k, v := range jobProp {
			propertyStr = append(propertyStr, fmt.Sprintf("\"%s\"=\"%s\"", k, v))
		}
		builder.WriteString(strings.Join(propertyStr, ","))
		builder.WriteString(")")
	}
	builder.WriteString(" " + req.GetCode().GetSql().GetCode())
	logger.Info().Msg(fmt.Sprintf("the execute message is %s", builder.String()))
	paragraphId, err := exec.zeppelinClient.AddParagraph(ctx, noteId, "code", builder.String())
	if err != nil {
		return "", "", nil, err
	}
	return noteId, paragraphId, result, nil
}

func (exec *FlinkExecutor) initJar(ctx context.Context, req *request.InitFlinkJob, udfs []*Udf, logger *glog.Logger) (string, string, *zeppelin.ParagraphResult, error) {
	properties := map[string]string{}
	properties["shell.command.timeout.millisecs"] = "30000"
	noteId, err := exec.initNote(ctx, SHELL, req.GetInstanceId(), properties)
	if err != nil {
		return "", "", nil, err
	}
	flinkUrl, flinkVersion, err := exec.getFlinkMessage(ctx, req.SpaceId, req.GetArgs().GetClusterId())
	if err != nil {
		return "", "", nil, err
	}
	runBuilder := strings.Builder{}
	flinkHome := constants.FlinkClientHome[flinkVersion]
	runBuilder.WriteString("%sh ")
	runBuilder.WriteString(fmt.Sprintf("%s/bin/flink run -d -m %s", flinkHome, flinkUrl))
	jarName, jarUrl, err := exec.resourceClient.GetFileById(ctx, req.GetCode().GetJar().GetResourceId())
	if err != nil {
		return "", "", nil, err
	}
	initBuilder := strings.Builder{}
	localJarPath := "/tmp/" + jarName
	initBuilder.WriteString(fmt.Sprintf("hdfs dfs -get %v %v\n", jarUrl, localJarPath))
	udfJars := exec.getUDFJars(req.GetSpaceId(), udfs)
	logger.Info().Msg(fmt.Sprintf("jar's udfJars is %s", udfJars))
	for index, udf := range strings.Split(udfJars, ",") {
		if len(udf) == 20 {
			var src = fmt.Sprintf("/tmp/%v-%d.jar", udf, index)
			initBuilder.WriteString(fmt.Sprintf("hdfs dfs -get %v %v\n", udf, src))
			runBuilder.WriteString(fmt.Sprintf(" -C %s", src))
		}
	}

	if req.GetArgs().GetParallelism() > 0 {
		runBuilder.WriteString(fmt.Sprintf(" -p %d", req.GetArgs().GetParallelism()))
	}
	if req.GetCode().GetJar().GetJarEntry() != "" && len(req.GetCode().GetJar().GetJarEntry()) > 0 {
		runBuilder.WriteString(fmt.Sprintf(" -c %s", req.GetCode().GetJar().GetJarEntry()))
	}
	runBuilder.WriteString(fmt.Sprintf(" %s %s", localJarPath, req.GetCode().GetJar().GetJarArgs()))
	initCode := initBuilder.String()
	logger.Info().Msg(fmt.Sprintf("the jar job init code is %s", initCode)).Fire()
	runCode := runBuilder.String()
	logger.Info().Msg(fmt.Sprintf("the jar job run code is %s", runCode)).Fire()
	result, err := exec.zeppelinClient.Execute(ctx, "sh", "", noteId, initCode)
	if err != nil {
		return "", "", nil, err
	} else if result != nil && !result.Status.IsFinished() {
		for _, re := range result.Results {
			if strings.EqualFold(re.Type, "TEXT") {
				logger.Warn().Msg(fmt.Sprintf("init jar failed,reason is %s", re.Data)).Fire()
			}
		}
		return "", "", nil, qerror.ZeppelinInitFailed
	}
	paragraphId, err := exec.zeppelinClient.AddParagraph(ctx, noteId, "code", runCode)
	if err != nil {
		return "", "", nil, err
	}
	return noteId, paragraphId, result, nil
}

func (exec *FlinkExecutor) runSql(ctx context.Context, instanceId string, noteId string, paragraphId string, logger *glog.Logger) (*zeppelin.ParagraphResult, error) {
	result, err := exec.zeppelinClient.SubmitParagraph(ctx, noteId, paragraphId)
	if err != nil {
		return nil, err
	}
	logger.Info().Msg(fmt.Sprintf("flink job submit finish, tmp status is %s,result is %v.", result.Status, result.Results)).Fire()
	if err = exec.preHandle(ctx, instanceId, noteId, result.ParagraphId); err != nil {
		return nil, err
	}
	logger.Info().Msg(fmt.Sprintf("insert pre submit success, instance_id is %s ,note_id is %s ,paragraph_id is %s", instanceId, noteId, result.ParagraphId)).Fire()
	defer func() {
		if err == nil {
			if result != nil && len(noteId) > 0 {
				// TODO 如果结束状态不是Running，去结果找是否是因为连接Flink超时导致的异常，如果只设置err为 FlinkRestError，返回
				if !result.Status.IsRunning() {
					for _, re := range result.Results {
						if strings.EqualFold(re.Type, "TEXT") && strings.Contains(re.Data, "Caused by: java.net.ConnectException: Connection refused") {
							logger.Error().Msg(fmt.Sprintf("flink cluster rest time out,job instanceId is %s", instanceId)).Fire()
							err = qerror.FlinkRestError.Format(500, "", "Caused by: java.net.ConnectException: Connection refused")
							return
						}
					}
					//TODO 如果结果状态为Running，但是JobId长度不符合
				} else if len(result.JobId) != 32 {
					result.Status = zeppelin.ERROR
				}
			}
			// TODO 如果没有异常，则结果写如数据库
			exec.postHandle(ctx, instanceId, noteId, result)
		}
	}()
	for {
		if result, err = exec.zeppelinClient.QueryParagraphResult(ctx, noteId, result.ParagraphId); err != nil {
			return nil, err
		}
		logger.Info().Msg(fmt.Sprintf("query result for instance %s until submit success, status %s, result %v", instanceId, result.Status, result.Results)).Fire()
		if result.Status.IsFailed() {
			return result, nil
		}
		if len(result.JobUrls) > 0 {
			jobUrl := result.JobUrls[0]
			if len(jobUrl)-1-strings.LastIndex(jobUrl, "/") == 32 {
				result.JobId = jobUrl[strings.LastIndex(jobUrl, "/")+1:]
				logger.Info().Msg(fmt.Sprintf("fetch flink job id success,flink job id is %s", result.JobId)).Fire()
			}
			return result, nil
		}
		time.Sleep(time.Second * 10)
	}
}

func (exec *FlinkExecutor) runJar(ctx context.Context, instanceId string, noteId string, paragraphId string, logger *glog.Logger) (*zeppelin.ParagraphResult, error) {
	result, err := exec.zeppelinClient.SubmitParagraph(ctx, noteId, paragraphId)
	if err != nil {
		return nil, err
	}
	logger.Info().Msg(fmt.Sprintf("flink job submit finish, tmp status is %s,result is %v.", result.Status, result.Results)).Fire()
	if err = exec.preHandle(ctx, instanceId, noteId, result.ParagraphId); err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			if result != nil && len(noteId) > 0 {
				// TODO 如果不是Running状态，判断是否是Flink集群导致的异常，设置异常，返回
				if !result.Status.IsRunning() {
					for _, re := range result.Results {
						// 如果 返回没有异常
						if strings.EqualFold(re.Type, "TEXT") && strings.Contains(re.Data, "Caused by: java.net.ConnectException: Connection refused") {
							logger.Error().Msg(fmt.Sprintf("flink cluster rest time out,job instanceId is %s", instanceId))
							err = qerror.FlinkRestError
							return
						}
					}
				}
			}
			// TODO 如果没有异常，则结果写如数据库
			exec.postHandle(ctx, instanceId, noteId, result)
		}
	}()
	for {
		if result, err = exec.zeppelinClient.QueryParagraphResult(ctx, noteId, result.ParagraphId); err != nil {
			return nil, err
		}
		logger.Info().Msg(fmt.Sprintf("query result for instance %s , status %s, result %v", instanceId, result.Status, result.Results))
		//TODO 判断是否为Running或Pending
		if !result.Status.IsRunning() && !result.Status.IsPending() {
			//TODO 判断是否拿到jobId
			if result.Results != nil && len(result.Results) > 0 {
				for _, re := range result.Results {
					jobInfo := strings.Split(re.Data, "JobID ")
					if len(jobInfo) == 2 {
						res := strings.Split(jobInfo[1], "\n")
						if len(res) > 0 && len(res[0]) == 32 {
							result.JobId = res[0]
							result.Status = zeppelin.RUNNING
							return result, nil
						}
					}
				}
				//TODO 拿不到返回ERROR
				result.Status = zeppelin.ERROR
			}
			return result, nil
		}
		time.Sleep(time.Second * 10)
	}
}

func (exec *FlinkExecutor) transResult(result *zeppelin.ParagraphResult) (string, model.StreamJobInst_State) {
	var data string
	var status model.StreamJobInst_State
	for _, re := range result.Results {
		if strings.EqualFold("TEXT", re.Type) {
			data = re.Data
		}
	}
	switch result.Status {
	case zeppelin.RUNNING:
		status = model.StreamJobInst_Running
	case zeppelin.ABORT, zeppelin.FINISHED:
		status = model.StreamJobInst_Succeed
	case zeppelin.ERROR:
		status = model.StreamJobInst_Failed
	default:
		status = model.StreamJobInst_Running
	}
	return data, status
}

func (exec *FlinkExecutor) preCheck(ctx context.Context, instanceId string) (*zeppelin.ParagraphResult, error) {
	if jobInfo, err := exec.getResult(ctx, instanceId); err != nil && !errors.Is(err, qerror.ResourceNotExists) {
		return nil, err
	} else if jobInfo != nil {
		// TODO 如果数据库有数据，则判断是否是running或finished 并且是否有flink job id，满足既返回
		if len(jobInfo.FlinkId) == 32 {
			result := zeppelin.ParagraphResult{
				NoteId:      jobInfo.NoteId,
				ParagraphId: jobInfo.ParagraphId,
				Status:      zeppelin.RUNNING,
				Progress:    0,
				Results:     nil,
				JobUrls:     nil,
				JobId:       jobInfo.FlinkId,
			}
			return &result, err
			// TODO 删除notebook
		}
	}
	return nil, nil
}

func (exec *FlinkExecutor) preHandle(ctx context.Context, instanceId string, noteId string, paragraphId string) error {
	jobInfo := model.JobInfo{
		InstanceId:  instanceId,
		NoteId:      noteId,
		ParagraphId: paragraphId,
		FlinkId:     "",
	}
	return exec.upsertResult(ctx, &jobInfo)
}

func (exec *FlinkExecutor) postHandle(ctx context.Context, instanceId string, noteId string, result *zeppelin.ParagraphResult) {
	if result != nil && (len(result.Results) > 0 || len(result.JobId) == 32) {
		if len(result.JobId) != 32 && (result.Status.IsRunning() || result.Status.IsPending()) {
			result.Status = zeppelin.ABORT
		} else {
			data, state := exec.transResult(result)
			jobInfo := model.JobInfo{
				InstanceId:  instanceId,
				NoteId:      noteId,
				ParagraphId: result.ParagraphId,
				FlinkId:     result.JobId,
				Message:     data,
				State:       state,
			}
			if err := exec.upsertResult(ctx, &jobInfo); err != nil {
				result.Status = zeppelin.ABORT
			}
		}
	}
}

func (exec *FlinkExecutor) upsertResult(ctx context.Context, jobInfo *model.JobInfo) error {
	db := exec.db.WithContext(ctx)
	return db.Table(JobManager).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "instance_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"note_id", "paragraph_id", "flink_id", "state", "message"}),
	}).Create(jobInfo).Error
}

func (exec *FlinkExecutor) deleteResult(ctx context.Context, instanceId string) error {
	return exec.db.WithContext(ctx).Table(JobManager).Where("instance_id = ?", instanceId).Delete(&model.JobInfo{}).Error
}

func (exec *FlinkExecutor) getResult(ctx context.Context, instanceId string) (*model.JobInfo, error) {
	var jobInfo model.JobInfo
	db := exec.db.WithContext(ctx)
	if err := db.Table(JobManager).Clauses(clause.Locking{Strength: "UPDATE"}).Where("instance_id", instanceId).First(&jobInfo).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = qerror.ResourceNotExists
		}
		return nil, err
	}
	return &jobInfo, nil
}

func (exec *FlinkExecutor) getBaseConnectors(builtInConnectors []string, flinkVersion string) string {
	libDir := constants.FlinkDefaultConnectorPath[flinkVersion]
	var executeJars string
	connectorSet := map[string]string{}
	connectorJarMap := constants.FlinkConnectorJarMap[flinkVersion]
	for _, connector := range builtInConnectors {
		jars := connectorJarMap[connector]
		for _, jar := range jars {
			connectorSet[libDir+"/"+jar+","] = ""
		}
	}
	for jar := range connectorSet {
		executeJars += jar
	}
	executeJars = strings.TrimSuffix(executeJars, ",")
	return executeJars
}

func (exec *FlinkExecutor) getUserDefineConnectors(spaceId string, resIds []string) string {
	builder := strings.Builder{}
	for _, id := range resIds {
		builder.WriteString("hdfs://hdfs-k8s/" + spaceId + "/" + id + ".jar,")
	}
	executeJars := builder.String()
	if executeJars != "" && len(executeJars) > 0 {
		executeJars = strings.TrimSuffix(executeJars, ",")
	}
	return executeJars
}

func (exec *FlinkExecutor) getUDFs(ctx context.Context, udfIds []string) ([]*Udf, error) {
	var udfCodes []*Udf
	for _, udfId := range udfIds {
		var (
			udfLanguage model.UDFInfo_Language
			udfDefine   string
			udfName     string
		)
		udfLanguage, udfName, udfDefine, err := exec.udfClient.DescribeUdfManager(ctx, udfId)
		if err != nil {
			return nil, err
		}

		if udfLanguage != model.UDFInfo_Java {
			udfDefine = strings.Replace(udfDefine, UDFQuote, udfName, -1)
		}
		udf := Udf{
			udfType: udfLanguage,
			code:    udfDefine,
		}
		udfCodes = append(udfCodes, &udf)
	}
	return udfCodes, nil
}

func (exec *FlinkExecutor) getUDFJars(spaceId string, udfs []*Udf) string {
	builder := strings.Builder{}
	for _, udf := range udfs {
		if udf.udfType == model.UDFInfo_Java {
			builder.WriteString("hdfs://hdfs-k8s/" + spaceId + "/" + udf.code + ".jar,")
		}
	}
	udfJars := builder.String()
	if udfJars != "" && len(udfJars) > 0 {
		udfJars = strings.TrimSuffix(udfJars, ",")
	}
	return udfJars
}

func (exec *FlinkExecutor) registerUDF(ctx context.Context, noteId string, udfs []*Udf) (*zeppelin.ParagraphResult, error) {
	logger := glog.FromContext(ctx)
	var result *zeppelin.ParagraphResult
	var err error
	for _, udf := range udfs {
		switch udf.udfType {
		case model.UDFInfo_Scala:
			if result, err = exec.zeppelinClient.Submit(ctx, "flink", "", noteId, udf.code); err != nil {
				return nil, err
			}
			start := time.Now().Unix()
			for (start - time.Now().Unix()) < 10000 {
				if result, err = exec.zeppelinClient.QueryParagraphResult(ctx, noteId, result.ParagraphId); err != nil {
					return nil, err
				}
				if !result.Status.IsRunning() && !result.Status.IsPending() {
					break
				}
				time.Sleep(time.Second * 2)
			}
		case model.UDFInfo_Python:
			if result, err = exec.zeppelinClient.Submit(ctx, "flink", "ipyflink", noteId, udf.code); err != nil {
				return nil, err
			}
			start := time.Now().Unix()
			for (start - time.Now().Unix()) < 10000 {
				if result, err = exec.zeppelinClient.QueryParagraphResult(ctx, noteId, result.ParagraphId); err != nil {
					return nil, err
				}
				if !result.Status.IsRunning() && !result.Status.IsPending() {
					break
				}
				time.Sleep(time.Second * 2)
			}
		}
	}
	if result != nil && !result.Status.IsFinished() {
		logger.Warn().Msg("register udf function failed.")
	}
	return result, nil
}

func (exec *FlinkExecutor) getFlinkMessage(ctx context.Context, spaceId string, clusterId string) (string, string, error) {
	flinkUrl, flinkVersion, err := exec.engineClient.GetEngineInfo(ctx, spaceId, clusterId)
	if err != nil {
		return "", "", err
	}
	return flinkUrl, flinkVersion, nil
}

func (exec *FlinkExecutor) getGlobalProperties(ctx context.Context, info *request.InitFlinkJob, udfs []*Udf) (map[string]string, error) {
	var (
		properties   = map[string]string{}
		flinkUrl     string
		flinkVersion string
		err          error
	)
	if strings.EqualFold(os.Getenv("LOCAL"), "TRUE") {
		properties["FLINK_HOME"] = "/Users/apple/develop/bigdata/flink-1.12.5"
		properties["flink.execution.remote.host"] = "127.0.0.1"
		properties["flink.execution.remote.port"] = "8081"
		flinkVersion = "flink-1.12.3-scala_2.11"
	} else {
		if flinkUrl, flinkVersion, err = exec.getFlinkMessage(ctx, info.SpaceId, info.GetArgs().GetClusterId()); err != nil {
			return nil, err
		}
		properties["FLINK_HOME"] = constants.FlinkClientHome[flinkVersion]
		host := flinkUrl[:strings.Index(flinkUrl, ":")]
		port := flinkUrl[strings.Index(flinkUrl, ":")+1:]
		if host != "" && len(host) > 0 && port != "" && len(port) > 0 {
			properties["flink.execution.remote.host"] = host
			properties["flink.execution.remote.port"] = port
		} else {
			return nil, qerror.ParseEngineFlinkUrlFailed.Format(flinkUrl)
		}
	}
	properties["flink.execution.mode"] = "remote"
	properties["zeppelin.flink.concurrentBatchSql.max"] = "100000"
	properties["zeppelin.flink.concurrentStreamSql.max"] = "100000"

	var executionJars string
	baseConnectors := exec.getBaseConnectors(info.GetArgs().GetBuiltInConnectors(), flinkVersion)
	userConnectors := exec.getUserDefineConnectors(info.SpaceId, info.GetArgs().GetConnectors())
	if baseConnectors != "" && len(baseConnectors) > 0 {
		executionJars = baseConnectors + "," + userConnectors
	} else if userConnectors != "" && len(userConnectors) > 0 {
		executionJars = userConnectors
	}
	if executionJars != "" && len(executionJars) > 0 {
		properties["flink.execution.jars"] = executionJars
	}

	udfJars := exec.getUDFJars(info.GetSpaceId(), udfs)
	if udfJars != "" && len(udfJars) > 0 {
		properties["flink.udf.jars"] = udfJars
	}
	return properties, nil
}

func (exec *FlinkExecutor) initNote(ctx context.Context, interceptor string, instanceId string, properties map[string]string) (string, error) {
	logger := glog.FromContext(ctx)
	var result *zeppelin.ParagraphResult
	var noteId string
	var err error
	defer func() {
		if len(noteId) > 0 && (result == nil || result.Status != zeppelin.FINISHED) {
			_ = exec.zeppelinClient.DeleteNote(ctx, noteId)
			err = qerror.ZeppelinInitFailed
		}
	}()
	if noteId, err = exec.zeppelinClient.CreateNote(ctx, instanceId); err != nil {
		var notesMap map[string]string
		notesMap, err = exec.zeppelinClient.ListNotes(ctx)
		if err != nil {
			fmt.Println(err.Error())
			return "", err
		}
		logger.Warn().Msg(fmt.Sprintf("note id exists, notes map is %s", notesMap)).Fire()
		if len(notesMap["/"+instanceId]) > 0 {
			logger.Warn().Msg(fmt.Sprintf("delete notename is %s,noteid is %s", "/"+instanceId, notesMap["/"+instanceId])).Fire()
			if err = exec.zeppelinClient.DeleteNote(ctx, notesMap["/"+instanceId]); err != nil {
				logger.Warn().Msg(fmt.Sprintf("delete note failed,notename %s .noteid is %s,reason is %s", "/"+instanceId, notesMap["/"+instanceId], err.Error())).Fire()
			}
		}
		if noteId, err = exec.zeppelinClient.CreateNote(ctx, instanceId); err != nil {
			logger.Warn().Msg(fmt.Sprintf("recreate note failed,note name %s ,reason is %s", instanceId, err.Error())).Fire()
			return "", err
		}
	}
	////TODO 创建了notebook 就记录一下
	//if err = exec.preHandle(exec.ctx, instanceId, noteId, ""); err != nil {
	//	return "", err
	//}
	builder := strings.Builder{}
	builder.WriteString("%" + interceptor + ".conf\n")
	for k, v := range properties {
		builder.WriteString(fmt.Sprintf("%v %v\n", k, v))
	}
	confParagraphId, err := exec.zeppelinClient.AddParagraph(ctx, noteId, "conf", builder.String())
	if err != nil {
		logger.Warn().Msg(fmt.Sprintf("add conf paragraph failed,noteid %s ,reason is %s", noteId, err.Error())).Fire()
		return "", err
	}
	if _, err = exec.zeppelinClient.ExecuteParagraph(ctx, noteId, confParagraphId); err != nil {
		logger.Warn().Msg(fmt.Sprintf("execute conf paragraph failed,noteid %s ,reason is %s", noteId, err.Error())).Fire()
		return "", err
	}
	builder.Reset()
	builder.WriteString("%" + interceptor + "(init=true)")
	initParagraphId, err := exec.zeppelinClient.AddParagraph(ctx, noteId, "init", builder.String())
	if err != nil {
		logger.Warn().Msg(fmt.Sprintf("add init paragraph failed,note id %s ,reason is %s", noteId, err.Error())).Fire()
		return "", err
	}

	if result, err = exec.zeppelinClient.ExecuteParagraph(ctx, noteId, initParagraphId); err != nil {
		logger.Warn().Msg(fmt.Sprintf("execute init paragraph failed,note id %s ,reason is %s", noteId, err.Error())).Fire()
		return "", err
	}
	if !result.Status.IsFinished() {
		if len(result.Results) > 0 {
			for _, re := range result.Results {
				if strings.EqualFold("TEXT", re.Type) {
					logger.Warn().Msg(fmt.Sprintf("zeppelin init failed,reason is %s", re.Data)).Fire()
				}
			}
		}
		return "", qerror.ZeppelinInitFailed
	}
	return noteId, nil
}
