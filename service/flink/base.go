package flink

import (
	"context"
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
	"strings"
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
	zeppelinConfig zeppelin.ClientConfig
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
		zeppelinConfig: zeppelinConfig,
		jobInfos:       make(chan *model.JobInfo, 100),
	}
}

func (bm *BaseExecutor) TransResult(spaceId string, jobId string, executeResult *zeppelin.ExecuteResult) *model.JobInfo {
	var data string
	for _, re := range executeResult.Results {
		if strings.EqualFold("TEXT", re.Type) {
			data = re.Data
		}
	}
	jobInfo := model.JobInfo{
		SpaceId:     spaceId,
		JobId:       jobId,
		NoteId:      executeResult.SessionInfo.NoteId,
		SessionId:   executeResult.SessionInfo.SessionId,
		StatementId: executeResult.StatementId,
		Data:        data,
	}
	switch executeResult.Status {
	case zeppelin.ERROR:
		jobInfo.Status = model.JobInfo_Failed
	case zeppelin.ABORT:
		jobInfo.Status = model.JobInfo_Suspended
	case zeppelin.FINISHED:
		jobInfo.Status = model.JobInfo_Finished
	default:
		jobInfo.Status = model.JobInfo_Initializing
	}
	return &jobInfo
}

func (bm *BaseExecutor) UpsertResult(ctx context.Context, jobInfo *model.JobInfo) error {
	db := bm.db.WithContext(ctx)
	return db.Table(JobManager).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "job_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"session_id", "statement_id", "flink_id", "status", "data"}),
	}).Create(jobInfo).Error
}

func (bm *BaseExecutor) GetResult(ctx context.Context, instanceId string) (*model.JobInfo, error) {
	var jobInfo model.JobInfo
	db := bm.db.WithContext(ctx)
	if err := db.Table(JobManager).Clauses(clause.Locking{Strength: "UPDATE"}).Where("job_id", instanceId).First(&jobInfo).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = qerror.ResourceNotExists.Format(instanceId)
		}
		return nil, err
	}
	return &jobInfo, nil
}

func (bm *BaseExecutor) getConnectors(builtInConnectors []string, flinkVersion string) string {
	var executeJars string
	var connectorSet map[string]string
	connectorJarMap := constants.FlinkConnectorJarMap[flinkVersion]
	for _, connector := range builtInConnectors {
		jars := connectorJarMap[connector]
		for _, jar := range jars {
			connectorSet[jar+","] = ""
		}
	}
	for jar, _ := range connectorSet {
		executeJars += jar
	}
	if executeJars != "" && len(executeJars) > 0 && strings.HasSuffix(executeJars, ",") {
		executeJars = executeJars[:strings.LastIndex(executeJars, ",")-1]
	}
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
	if strings.HasSuffix(executionUdfJars, ",") {
		executionUdfJars = executionUdfJars[:len(executionUdfJars)-1]
	}
	return executionUdfJars
}

func (bm *BaseExecutor) getGlobalProperties(ctx context.Context, info *request.RunJob, udfs []*Udf) (map[string]string, error) {
	properties := map[string]string{}
	properties["FLINK_HOME"] = "/Users/apple/develop/bigdata/flink-1.12.5"

	//flinkUrl, flinkVersion, err := bm.engineClient.GetEngineInfo(ctx, info.GetSpaceId(), info.GetArgs().GetClusterId())
	//if err != nil {
	//	return nil, err
	//}
	//host := flinkUrl[:strings.Index(flinkUrl, ":")]
	//port := flinkUrl[strings.Index(flinkUrl, ":")+1:]
	//if host != "" && len(host) > 0 && port != "" && len(port) > 0 {
	//	properties["flink.execution.remote.host"] = host
	//	properties["flink.execution.remote.port"] = port
	//} else {
	//	return nil, qerror.ParseEngineFlinkUrlFailed.Format(flinkUrl)
	//}

	properties["flink.execution.mode"] = "remote"
	properties["flink.execution.remote.host"] = "127.0.0.1"
	properties["flink.execution.remote.port"] = "8081"

	flinkVersion := "flink-1.12.3-scala_2.11"
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

func (bm *BaseExecutor) GetJobInfo(ctx context.Context, instanceId string, spaceId string, clusterId string) (*flink.Job, error) {
	//flinkUrl, _, err := bm.engineClient.GetEngineInfo(ctx, spaceId, clusterId)
	//if err != nil {
	//	return nil, err
	//}

	flinkUrl := "127.0.0.1:8081"
	result, err := bm.GetResult(ctx, instanceId)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// TODO 没有这条记录
		}
		return nil, err
	}
	if len(result.FlinkId) != 32 {
		// TODO 记录下来
	}
	return bm.flinkClient.GetJobInfoByJobId(flinkUrl, result.FlinkId)
}

func (bm *BaseExecutor) CancelJob(ctx context.Context, instanceId string, spaceId string, clusterId string) error {
	//flinkUrl, _, err := bm.engineClient.GetEngineInfo(ctx, spaceId, clusterId)
	//if err != nil {
	//	return err
	//}

	flinkUrl := "127.0.0.1:8081"
	result, err := bm.GetResult(ctx, instanceId)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			//TODO 没有这条记录
		}
		return err
	}
	if len(result.FlinkId) != 32 {
		// TODO 记录下来,返回失败
	}
	return bm.flinkClient.CancelJob(flinkUrl, result.FlinkId)
}
