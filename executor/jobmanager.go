package executor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/jobpb"

	"gorm.io/gorm"
)

const (
	StatusFailed        = constants.InstanceStateFailed
	StatusFailedString  = "failed"
	StatusFinish        = constants.InstanceStateSucceed
	StatusFinishString  = "finish"
	StatusRunning       = constants.InstanceStateRunning
	StatusRunningString = "running"
	JobSuccess          = "success"
	JobAbort            = "job abort"
	JobRunning          = "job running"
	jobError            = "error happend"

	JobmanagerTableName = "jobmanager"
	MaxStatusFailedNum  = 100
)

type JobResources struct {
	Jar string `json:"jar"`
}

type ParagraphsInfo struct {
	Conf    string `json:"conf"`
	Depends string `json:"depends"`
	MainRun string `json:"mainrun"`
}

type JobmanagerInfo struct {
	ID         string `gorm:"column:id;primaryKey"`
	NoteID     string `gorm:"column:noteid;"`
	Status     string `gorm:"column:status;"`
	Message    string `gorm:"column:message;"`
	Paragraph  string `gorm:"column:paragraph;"`
	CreateTime string `gorm:"column:createtime;"`
	UpdateTime string `gorm:"column:updatetime;"`
	Resources  string `gorm:"column:resources;"`
	SpaceID    string `gorm:"column:spaceid;"`
}

type EnginOptions struct {
	JobID       string  `json:"jobid"`
	WorkspaceID string  `json:"workspaceid"`
	Parallelism int32   `json:"parallelism"`
	JobMem      int32   `json:"job_mem"` // in MB
	JobCpu      float32 `json:"job_cpu"`
	TaskCpu     float32 `json:"task_cpu"`
	TaskMem     int32   `json:"task_mem"` // in MB
	TaskNum     int32   `json:"task_num"`
	AccessKey   string  `json:"accesskey"`
	SecretKey   string  `json:"secretkey"`
	EndPoint    string  `json:"endpoint"`
}

type JobWatchInfo struct {
	ID           string
	NoteID       string
	ParagraphIDs ParagraphsInfo
	Resources    JobResources
}

type jobQueueType struct {
	Job             JobWatchInfo
	RunEnd          bool
	ParagraphIndex  int
	ParagraphID     string
	StatusFailedNum int32
}

func (smi JobmanagerInfo) TableName() string {
	return JobmanagerTableName
}

type JobmanagerExecutor struct {
	db           *gorm.DB
	idGenerator  *idgenerator.IDGenerator
	httpClient   HttpClient
	sourceClient SourceClient
	watchChan    chan JobWatchInfo
	ctx          context.Context
	logger       *glog.Logger
}

func NewJobManagerExecutor(db *gorm.DB, client HttpClient, sClient SourceClient, jobwork int32, ictx context.Context, logger *glog.Logger) *JobmanagerExecutor {
	ex := &JobmanagerExecutor{
		db:           db,
		idGenerator:  idgenerator.New(constants.JobIDPrefix),
		httpClient:   client,
		sourceClient: sClient,
		watchChan:    make(chan JobWatchInfo, jobwork),
		ctx:          ictx,
		logger:       logger,
	}

	for i := int32(0); i < jobwork; i++ {
		go ex.WatchJob(ex.ctx)
	}

	ex.PickupAloneJob(ex.ctx)
	return ex
}

func StringStatusToInt32(s string) (r int32) {
	if s == StatusRunningString {
		r = StatusRunning
	} else if s == StatusFinishString {
		r = StatusFinish
	} else if s == StatusFailedString {
		r = StatusFailed
	}
	return r
}

func Int32StatusToString(i int32) (r string) {
	if i == StatusRunning {
		r = StatusRunningString
	} else if i == StatusFinish {
		r = StatusFinishString
	} else if i == StatusFailed {
		r = StatusFailedString
	}
	return r
}

func (ex *JobmanagerExecutor) ModifyStatus(ctx context.Context, ID string, status int32, message string, resources JobResources) (err error) {
	var info JobmanagerInfo

	info.ID = ID
	info.Status = Int32StatusToString(status)
	info.Message = message
	info.UpdateTime = time.Now().Format("2006-01-02 15:04:05")

	db := ex.db.WithContext(ctx)
	if err = db.Select("status", "message", "updatetime").Where("id = ? ", info.ID).Updates(info).Error; err != nil {
		return
	}

	if status == StatusFinish || status == StatusFailed {
		//TODO delete jar, if len > 0
		ex.logger.Info().Msg("delete jar").String("jar", resources.Jar).Fire()

		FreeEngine(ID)
	}

	return
}

func (ex *JobmanagerExecutor) RunJob(ctx context.Context, ID string, WorkspaceID string, NodeType int32, Depends string) (rep jobpb.JobReply, err error) {
	var watchInfo JobWatchInfo
	err, watchInfo = ex.RunJobUtile(ctx, ID, WorkspaceID, NodeType, Depends)
	if err != nil {
		ex.logger.Error().Msg("can't run job").String("jobid", ID).Error("", err).Fire()
		if watchInfo.NoteID != "" {
			_ = ex.httpClient.DeleteNote(watchInfo.NoteID)
		}
		rep.Status = StatusFailed
		rep.Message = err.Error()
		return
	}

	ex.watchChan <- watchInfo

	// conf
	if err = ex.httpClient.RunParagraphSync(watchInfo.NoteID, watchInfo.ParagraphIDs.Conf); err != nil {
		return
	}

	// depend
	if err = ex.httpClient.RunParagraphSync(watchInfo.NoteID, watchInfo.ParagraphIDs.Depends); err != nil {
		return
	}

	// main fun
	if err = ex.httpClient.RunParagraphAsync(watchInfo.NoteID, watchInfo.ParagraphIDs.MainRun); err != nil {
		return
	}

	rep.Status = StatusRunning
	rep.Message = JobRunning

	return
}

func (ex *JobmanagerExecutor) RunJobUtile(ctx context.Context, ID string, WorkspaceID string, NodeType int32, Depends string) (err error, watchInfo JobWatchInfo) {
	var (
		info       JobmanagerInfo
		Pa         ParagraphsInfo
		engineOpts EnginOptions

		zeplinConf    string
		zeplinDepends string
		zeplinMainRun string
		resources     JobResources
	)

	info.ID = ID
	watchInfo.ID = info.ID
	info.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	info.UpdateTime = info.CreateTime
	info.Status = StatusRunningString
	info.Message = JobRunning
	info.SpaceID = WorkspaceID

	engineOpts.JobID = info.ID
	engineOpts.WorkspaceID = WorkspaceID

	if NodeType == constants.NodeTypeFlinkSSQL {
		var v constants.FlinkSSQL
		var s3info constants.SourceS3Params

		if err = json.Unmarshal([]byte(Depends), &v); err != nil {
			return
		}
		if s3info, err = GetS3Info(ex.sourceClient, Depends); err != nil {
			return
		}
		engineOpts.Parallelism = v.Parallelism
		engineOpts.JobCpu = v.JobCpu
		engineOpts.JobMem = v.JobMem
		engineOpts.TaskCpu = v.TaskCpu
		engineOpts.TaskMem = v.TaskMem
		engineOpts.TaskNum = v.TaskNum
		engineOpts.AccessKey = s3info.AccessKey
		engineOpts.SecretKey = s3info.SecretKey
		engineOpts.EndPoint = s3info.EndPoint
	} else if NodeType == constants.NodeTypeFlinkJob {
		var v constants.FlinkJob
		if err = json.Unmarshal([]byte(Depends), &v); err != nil {
			return
		}
		engineOpts.Parallelism = v.Parallelism
		engineOpts.JobCpu = v.JobCpu
		engineOpts.JobMem = v.JobMem
		engineOpts.TaskCpu = v.TaskCpu
		engineOpts.TaskMem = v.TaskMem
		engineOpts.TaskNum = v.TaskNum
		engineOpts.AccessKey = v.AccessKey
		engineOpts.SecretKey = v.SecretKey
		engineOpts.EndPoint = v.EndPoint
	}

	engineOptsByte, _ := json.Marshal(engineOpts)
	ex.logger.Debug().String("engine options", string(engineOptsByte)).Fire()
	engineType, engineHost, enginePort, _, tmperr := GetEngine(string(engineOptsByte))
	if tmperr != nil {
		err = tmperr
		return
	}

	info.NoteID, err = ex.httpClient.CreateNote(ID)
	if err != nil {
		return
	}
	watchInfo.NoteID = info.NoteID

	if engineType == constants.EngineTypeFlink {
		zeplinConf = GenerateFlinkConf(ex.httpClient.ZeppelinFlinkHome, ex.httpClient.ZeppelinFlinkExecuteJars, engineHost, enginePort, NodeType)
	}
	Pa.Conf, err = ex.httpClient.CreateParagraph(info.NoteID, 0, "JobConf", zeplinConf)
	if err != nil {
		return
	}

	// depend and mainrun text
	if engineType == constants.EngineTypeFlink {
		zeplinDepends, zeplinMainRun, resources, err = GenerateFlinkJob(ex.sourceClient, ex.httpClient.ZeppelinFlinkHome, engineHost+":"+enginePort, NodeType, Depends)
	}
	if err != nil {
		return
	}
	resourcesByte, _ := json.Marshal(resources)
	info.Resources = string(resourcesByte)
	watchInfo.Resources = resources

	Pa.Depends, err = ex.httpClient.CreateParagraph(info.NoteID, 1, "JobDepends", zeplinDepends)
	if err != nil {
		return
	}

	Pa.MainRun, err = ex.httpClient.CreateParagraph(info.NoteID, 2, "JobMainrun", zeplinMainRun)
	if err != nil {
		return
	}

	PaByte, _ := json.Marshal(Pa)
	info.Paragraph = string(PaByte)
	watchInfo.ParagraphIDs = Pa

	db := ex.db.WithContext(ctx)
	err = db.Create(info).Error
	return
}

func GetNextParagraphID(job jobQueueType) (r jobQueueType) {
	order := make(map[int]string)
	order[0] = job.Job.ParagraphIDs.Conf
	order[1] = job.Job.ParagraphIDs.Depends
	order[2] = job.Job.ParagraphIDs.MainRun

	r = job

	if job.ParagraphIndex == 2 {
		r.RunEnd = true
	} else {
		r.ParagraphIndex = job.ParagraphIndex + 1
		r.ParagraphID = order[r.ParagraphIndex]
	}
	return
}

func InitJobInfo(watchInfo JobWatchInfo) (job jobQueueType) {
	job.Job = watchInfo
	job.ParagraphIndex = 0
	job.ParagraphID = watchInfo.ParagraphIDs.Conf
	job.RunEnd = false
	job.StatusFailedNum = 0

	return
}

func (ex *JobmanagerExecutor) WatchJob(ctx context.Context) {
	var (
		err    error
		status string
	)

	jobQueue := make(map[string]jobQueueType)

	for true {
		select {
		case info := <-ex.watchChan:
			jobQueue[info.ID] = InitJobInfo(info)
			if err = ex.ModifyStatus(ctx, info.ID, StatusRunning, "ready to checkstatus", info.Resources); err != nil {
				ex.logger.Error().Msg("can't change the job status to  running").String("jobid", info.ID).Fire()
			}
		case <-time.After(time.Second * 1):
			for id, job := range jobQueue {
				for true {
					if status, err = ex.httpClient.GetParagraphStatus(job.Job.NoteID, job.ParagraphID); err != nil {
						job.StatusFailedNum += 1
						jobQueue[id] = job
						ex.logger.Error().Msg("can't get this paragraph status").String("noteid", job.Job.NoteID).String("paragraphid", job.ParagraphID).Int32("failednum", job.StatusFailedNum).Fire()

						if job.StatusFailedNum < MaxStatusFailedNum {
							break
						} else {
							status = ParagraphError
							err = nil
						}
					}
					if status == ParagraphFinish {
						job = GetNextParagraphID(job)
						if job.RunEnd == true {
							if err = ex.ModifyStatus(ctx, job.Job.ID, StatusFinish, JobSuccess, job.Job.Resources); err != nil {
								ex.logger.Error().Msg("can't change the job status to finish").String("jobid", job.Job.ID).Fire()
								break
							}

							if err = ex.httpClient.DeleteNote(job.Job.NoteID); err != nil {
								ex.logger.Error().Msg("can't delete the job note").String("jobid", job.Job.ID).Fire()
							}
							delete(jobQueue, id)
							break
						}
						jobQueue[id] = job
						if err = ex.ModifyStatus(ctx, job.Job.ID, StatusRunning, job.ParagraphID+" is running", job.Job.Resources); err != nil {
							ex.logger.Error().Msg("can't change the job status to running").String("jobid", job.Job.ID).String("paragraphid", job.ParagraphID).Fire()
							break
						}
					} else if status == ParagraphError {
						var joberrmsg string

						if joberrmsg, err = ex.httpClient.GetParagraphErrorMsg(job.Job.NoteID, job.ParagraphID); err != nil {
							ex.logger.Error().Msg("can't get this paragraph info for a error paragraph").String("noteid", job.Job.NoteID).String("paragraphid", job.ParagraphID).String("error msg", err.Error()).Fire()
							joberrmsg = "get error message failed"
						}

						if err = ex.ModifyStatus(ctx, job.Job.ID, StatusFailed, joberrmsg, job.Job.Resources); err != nil {
							ex.logger.Error().Msg("can't change the job status to failed").String("jobid", job.Job.ID).Fire()
							break
						}
						if job.StatusFailedNum < MaxStatusFailedNum {
							if err = ex.httpClient.DeleteNote(job.Job.NoteID); err != nil {
								ex.logger.Error().Msg("can't delete the job note").String("jobid", job.Job.ID).Fire()
							}
						} else {
							ex.logger.Warn().Msg("don't delete the job note").String("jobid", job.Job.ID).Fire()
						}
						delete(jobQueue, id)
						break
					} else if status == ParagraphAbort {
						if err = ex.ModifyStatus(ctx, job.Job.ID, StatusFinish, JobAbort, job.Job.Resources); err != nil {
							ex.logger.Error().Msg("can't change the job status to finish(abort)").String("jobid", job.Job.ID).Fire()
							break
						}

						if err = ex.httpClient.DeleteNote(job.Job.NoteID); err != nil {
							ex.logger.Error().Msg("can't delete the job note").String("jobid", job.Job.ID).Fire()
						}
						delete(jobQueue, id)
						break
					} else {
						/* paragraph is running
						ParagraphUnknown = "UNKNOWN"
						ParagraphRunning = "RUNNING"
						ParagraphReady = "READY"
						ParagraphPending = "PENDING"
						*/
						break
					}
				}
			}
		}
	}
}

func (ex *JobmanagerExecutor) PickupAloneJob(ctx context.Context) {
	var (
		err  error
		jobs []JobmanagerInfo
	)

	db := ex.db.WithContext(ctx)
	if err = db.Table(JobmanagerTableName).Select("id, noteid, paragraph,resources").Where("status = '" + StatusRunningString + "'").Scan(&jobs).Error; err != nil {
		ex.logger.Error().Msg("can't scan jobmanager table for pickup alone job").Fire()
		return
	}

	for _, job := range jobs {
		var watchInfo JobWatchInfo
		var Pa ParagraphsInfo
		var r JobResources

		watchInfo.ID = job.ID
		watchInfo.NoteID = job.NoteID
		if err = json.Unmarshal([]byte(job.Paragraph), &Pa); err != nil {
			return
		}
		watchInfo.ParagraphIDs = Pa
		if err = json.Unmarshal([]byte(job.Resources), &r); err != nil {
			return
		}
		watchInfo.Resources = r
		ex.logger.Info().Msg("pickup alone job").String("jobid", job.ID).Fire()

		ex.watchChan <- watchInfo
	}

	return
}

func (ex *JobmanagerExecutor) GetJobStatus(ctx context.Context, ID string) (rep jobpb.JobReply, err error) {
	job, tmperr := ex.GetJobInfo(ctx, ID)

	if tmperr != nil {
		err = tmperr
		return
	}

	rep.Status = StringStatusToInt32(job.Status)
	rep.Message = job.Message
	return
}

func (ex *JobmanagerExecutor) GetJobInfo(ctx context.Context, ID string) (job JobmanagerInfo, err error) {
	db := ex.db.WithContext(ctx)
	err = db.Table(JobmanagerTableName).Select("noteid, status,message").Where("id = '" + ID + "'").Scan(&job).Error
	return
}

func (ex *JobmanagerExecutor) CancelJob(ctx context.Context, ID string) (err error) {
	job, tmperr := ex.GetJobInfo(ctx, ID)
	ex.logger.Warn().Any("", job).Fire()

	if tmperr != nil {
		err = tmperr
		return
	}
	ex.logger.Warn().Msg("user cancel job").String("id", ID).Fire() // if use cancel.  log is necessary
	err = ex.httpClient.StopAllParagraphs(job.NoteID)
	//TODO jar. cancel trigger savepoint
	return
}

func (ex *JobmanagerExecutor) CancelAllJob(ctx context.Context, SpaceID string) (err error) {
	var (
		jobs []JobmanagerInfo
	)

	db := ex.db.WithContext(ctx)
	if err = db.Table(JobmanagerTableName).Select("id").Where("spaceid = '" + SpaceID + "' and status = '" + StatusRunningString + "'").Scan(&jobs).Error; err != nil {
		ex.logger.Error().Msg("can't scan jobmanager table for cancel all job").Fire()
		return
	}

	for _, job := range jobs {
		tmperr := ex.CancelJob(ctx, job.ID)
		if tmperr == nil {
			ex.logger.Info().String("cancel all running job for spaceid", SpaceID).String("jobid", job.ID).Fire()
		} else {
			ex.logger.Error().String("cancel all running job for spaceid", SpaceID).String("jobid", job.ID).Fire()
		}
	}

	return
}
