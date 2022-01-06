package flink

import (
	"context"
	"fmt"
	"strings"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/jobmanager/utils"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	Quote      = "$qc$"
	UDFQuote   = Quote + "_udf_name_" + Quote
	JobManager = "job_manager"
)

const (
	FLINK = "flink"
	SHELL = "sh"
)

type BaseExecutor struct {
	ctx            context.Context
	db             *gorm.DB
	logger         *glog.Logger
	engineClient   utils.EngineClient
	udfClient      utils.UdfClient
	resourceClient utils.ResourceClient
	flinkClient    *flink.Client
	zeppelinClient *zeppelin.Client
	jobInfos       chan *model.JobInfo
}

type Udf struct {
	udfType model.UDFInfo_Language
	code    string
}

func NewBaseManager(ctx context.Context, db *gorm.DB, logger *glog.Logger, engineClient utils.EngineClient, udfClient utils.UdfClient, resourceClient utils.ResourceClient,
	flinkConfig flink.ClientConfig, zeppelinConfig zeppelin.ClientConfig) *BaseExecutor {
	return &BaseExecutor{
		ctx:            ctx,
		db:             db,
		logger:         logger,
		engineClient:   engineClient,
		udfClient:      udfClient,
		resourceClient: resourceClient,
		flinkClient:    flink.NewFlinkClient(flinkConfig),
		zeppelinClient: zeppelin.NewZeppelinClient(zeppelinConfig),
		jobInfos:       make(chan *model.JobInfo, 100),
	}
}

func TransResult(result *zeppelin.ParagraphResult) (string, model.StreamJobInst_State) {
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
	}
	return data, status
}

func (bm *BaseExecutor) preCheck(ctx context.Context, instanceId string) (*zeppelin.ParagraphResult, error) {
	if jobInfo, err := bm.getResult(ctx, instanceId); err != nil && !errors.Is(err, qerror.ResourceNotExists) {
		return nil, err
	} else if jobInfo != nil {
		if jobInfo.State == model.StreamJobInst_Running || jobInfo.State == model.StreamJobInst_Succeed && len(jobInfo.FlinkId) == 32 {
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
		} else if len(jobInfo.NoteId) > 0 {
			_ = bm.zeppelinClient.DeleteNote(jobInfo.NoteId)
		}
	}
	return nil, nil
}

func (bm *BaseExecutor) PreHandle(ctx context.Context, instanceId string, noteId string, paragraphId string) error {
	jobInfo := model.JobInfo{
		InstanceId:  instanceId,
		NoteId:      noteId,
		ParagraphId: paragraphId,
		FlinkId:     "",
	}
	return bm.upsertResult(ctx, &jobInfo)
}

func (bm *BaseExecutor) PostHandle(ctx context.Context, instanceId string, noteId string,
	result *zeppelin.ParagraphResult) {
	if result != nil && (len(result.Results) > 0 || len(result.JobId) == 32) {
		if len(result.JobId) != 32 && (result.Status.IsRunning() || result.Status.IsPending()) {
			result.Status = zeppelin.ABORT
		} else {
			data, state := TransResult(result)
			jobInfo := model.JobInfo{
				InstanceId:  instanceId,
				NoteId:      noteId,
				ParagraphId: result.ParagraphId,
				FlinkId:     result.JobId,
				Message:     data,
				State:       state,
			}
			if err := bm.upsertResult(ctx, &jobInfo); err != nil {
				result.Status = zeppelin.ABORT
			}
		}
	}
}

func (bm *BaseExecutor) upsertResult(ctx context.Context, jobInfo *model.JobInfo) error {
	db := bm.db.WithContext(ctx)
	return db.Table(JobManager).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "instance_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"note_id", "paragraph_id", "flink_id", "state", "message"}),
	}).Create(jobInfo).Error
}

func (bm *BaseExecutor) deleteResult(ctx context.Context, instanceId string) error {
	return bm.db.WithContext(ctx).Table(JobManager).Where("instance_id = ?", instanceId).Delete(&model.JobInfo{}).Error
}

func (bm *BaseExecutor) getResult(ctx context.Context, instanceId string) (*model.JobInfo, error) {
	var jobInfo model.JobInfo
	db := bm.db.WithContext(ctx)
	if err := db.Table(JobManager).Clauses(clause.Locking{Strength: "UPDATE"}).Where("instance_id", instanceId).First(&jobInfo).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = qerror.ResourceNotExists
		}
		return nil, err
	}
	return &jobInfo, nil
}

func (bm *BaseExecutor) getConnectors(builtInConnectors []string, flinkVersion string) string {
	var libDir = "/zeppelin/flink/1.12_lib/"
	var executeJars string
	connectorSet := map[string]string{}
	connectorJarMap := constants.FlinkConnectorJarMap[flinkVersion]
	for _, connector := range builtInConnectors {
		jars := connectorJarMap[connector]
		for _, jar := range jars {
			connectorSet[libDir+jar+","] = ""
		}
	}
	for jar := range connectorSet {
		executeJars += jar
	}
	executeJars = strings.TrimSuffix(executeJars, ",")
	return executeJars
}

func (bm *BaseExecutor) getUDFs(ctx context.Context, udfIds []string) ([]*Udf, error) {
	var udfCodes []*Udf
	for _, udfId := range udfIds {
		var (
			udfLanguage model.UDFInfo_Language
			udfDefine   string
			udfName     string
		)
		udfLanguage, udfName, udfDefine, err := bm.udfClient.DescribeUdfManager(ctx, udfId)
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

func (bm *BaseExecutor) getUDFJars(udfs []*Udf) string {
	var executionUdfJars string
	for _, udf := range udfs {
		if udf.udfType == model.UDFInfo_Java {
			executionUdfJars += udf.code + ","
		}
	}
	executionUdfJars = strings.TrimSuffix(executionUdfJars, ",")
	return executionUdfJars
}

func (bm *BaseExecutor) registerUDF(noteId string, udfs []*Udf) (*zeppelin.ParagraphResult, error) {
	var result *zeppelin.ParagraphResult
	var err error
	for _, udf := range udfs {
		switch udf.udfType {
		case model.UDFInfo_Scala:
			result, err = bm.zeppelinClient.Execute("flink", "", noteId, udf.code)
			if err != nil {
				return nil, err
			}
		case model.UDFInfo_Python:
			result, err = bm.zeppelinClient.Execute("flink", "ipyflink", noteId, udf.code)
			if err != nil {
				return nil, err
			}
		}
	}
	if result != nil && !result.Status.IsFinished() {
		bm.logger.Warn().Msg("register udf function failed.")
	}
	return result, nil
}

func (bm *BaseExecutor) getGlobalProperties(ctx context.Context, info *request.RunJob, udfs []*Udf) (map[string]string, error) {
	properties := map[string]string{}
	properties["FLINK_HOME"] = "/zeppelin/flink/flink-1.12.3"

	flinkUrl, flinkVersion, err := bm.engineClient.GetEngineInfo(ctx, info.GetSpaceId(), info.GetArgs().GetClusterId())
	if err != nil {
		return nil, err
	}
	host := flinkUrl[:strings.Index(flinkUrl, ":")]
	port := flinkUrl[strings.Index(flinkUrl, ":")+1:]
	if host != "" && len(host) > 0 && port != "" && len(port) > 0 {
		properties["flink.execution.remote.host"] = host
		properties["flink.execution.remote.port"] = port
	} else {
		return nil, qerror.ParseEngineFlinkUrlFailed.Format(flinkUrl)
	}
	properties["flink.execution.mode"] = "remote"
	properties["zeppelin.flink.concurrentBatchSql.max"] = "100000"
	properties["zeppelin.flink.concurrentStreamSql.max"] = "100000"

	//properties["FLINK_HOME"] = "/Users/apple/develop/bigdata/flink-1.12.5"
	//properties["flink.execution.remote.host"] = "127.0.0.1"
	//properties["flink.execution.remote.port"] = "8081"
	//flinkVersion := "flink-1.12.3-scala_2.11"

	executionJars := bm.getConnectors(info.GetArgs().GetBuiltInConnectors(), flinkVersion)
	if executionJars != "" && len(executionJars) > 0 {
		properties["flink.execution.jars"] = executionJars
	}
	udfJars := bm.getUDFJars(udfs)
	if udfJars != "" && len(udfJars) > 0 {
		properties["flink.udf.jars"] = udfJars
	}
	return properties, nil
}

func (bm *BaseExecutor) initNote(interceptor string, instanceId string, properties map[string]string) (string, error) {
	noteId, err := bm.zeppelinClient.CreateNote(instanceId)
	var notesMap map[string]string
	if err != nil {
		if err == qerror.ZeppelinNoteAlreadyExists {
			notesMap, err = bm.zeppelinClient.ListNotes()
			if err != nil {
				return "", err
			}
			if len(notesMap[instanceId]) > 0 {
				_ = bm.zeppelinClient.DeleteNote(notesMap[instanceId])
			}
			noteId, err = bm.zeppelinClient.CreateNote(instanceId)
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}
	builder := strings.Builder{}
	builder.WriteString("%" + interceptor + ".conf\n")
	for k, v := range properties {
		builder.WriteString(fmt.Sprintf("%v %v\n", k, v))
	}
	confParagraphId, err := bm.zeppelinClient.AddParagraph(noteId, "conf", builder.String())
	if err != nil {
		return "", err
	}
	if _, err = bm.zeppelinClient.ExecuteParagraph(noteId, confParagraphId); err != nil {
		return "", err
	}
	builder.Reset()
	builder.WriteString("%" + interceptor + "(init=true)")
	initParagraphId, err := bm.zeppelinClient.AddParagraph(noteId, "init", builder.String())
	if err != nil {
		return "", err
	}
	var result *zeppelin.ParagraphResult
	if result, err = bm.zeppelinClient.ExecuteParagraph(noteId, initParagraphId); err != nil {
		return "", err
	}
	if !result.Status.IsFinished() {
		return "", qerror.ZeppelinInitFailed
	}
	return noteId, nil
}

func (bm *BaseExecutor) getJobInfo(ctx context.Context, instanceId string, spaceId string, clusterId string) (*flink.Job, error) {
	//flinkUrl := "127.0.0.1:8081"
	flinkUrl, _, err := bm.engineClient.GetEngineInfo(ctx, spaceId, clusterId)
	if err != nil {
		return nil, err
	}

	result, err := bm.getResult(ctx, instanceId)
	if err != nil {
		//if errors.Is(err, gorm.ErrRecordNotFound) {
		//	// TODO 没有这条记录
		//}
		return nil, err
	}
	//if len(result.FlinkId) != 32 {
	//	// TODO 记录下来
	//}
	return bm.flinkClient.GetJobInfoByJobId(flinkUrl, result.FlinkId)
}

func (bm *BaseExecutor) release(ctx context.Context, instanceId string) error {
	result, err := bm.getResult(ctx, instanceId)
	if err != nil {
		return err
	}
	if len(result.NoteId) > 0 {
		_ = bm.zeppelinClient.DeleteNote(result.NoteId)
	}
	return bm.deleteResult(ctx, instanceId)
}

func (bm *BaseExecutor) cancelJob(ctx context.Context, instanceId string, spaceId string, clusterId string) (err error) {
	var result *model.JobInfo
	//flinkUrl := "127.0.0.1:8081"
	flinkUrl, _, err := bm.engineClient.GetEngineInfo(ctx, spaceId, clusterId)
	if err != nil {
		return err
	}

	result, err = bm.getResult(ctx, instanceId)
	if err != nil {
		if errors.Is(err, qerror.ResourceNotExists) {
			//TODO 没有这条记录
			return nil
		}
		return err
	}
	//if len(result.FlinkId) != 32 {
	//	// TODO 记录下来,返回失败
	//}
	defer func() {
		if result != nil && len(result.NoteId) > 0 {
			_ = bm.zeppelinClient.DeleteNote(result.NoteId)
		}
	}()
	err = bm.flinkClient.CancelJob(flinkUrl, result.FlinkId)
	return err
}
