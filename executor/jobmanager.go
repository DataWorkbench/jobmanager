package executor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/jobpb"

	"gorm.io/gorm"
)

const (
	JobmanagerIDPrefix  = "job-"
	JobmanagerTableName = "jobmanager"

	StatusFailed  = "failed"
	StatusFinish  = "finish"
	StatusRunning = "running"
	JobSuccess    = "success"
	JobAbort      = "job abort"
	jobError      = "error happend"

	JobConf    = "conf"    //run first
	JobDepends = "depends" //run second
	JobMainrun = "mainrun" //run third

	FlinkJobSsql = "ssql"
	FlinkJobJar  = "jar"
)

type JobmanagerInfo struct {
	ID         string `gorm:"column:id;primaryKey"`
	NoteID     string `gorm:"column:noteid;"`
	Status     string `gorm:"column:status;"`
	Message    string `gorm:"column:message;"`
	Paragraph  string `gorm:"column:paragraph;"`
	CreateTime string `gorm:"column:createtime;"`
	UpdateTime string `gorm:"column:updatetime;"`
	Resources  string `gorm:"column:resources;"`
}

type JobWatchInfo struct {
	ID           string
	NoteID       string
	ParagraphIDs string
	Resources    string
}

type jobQueueType struct {
	ID             string
	NoteID         string
	RunEnd         bool
	ParagraphIndex int
	ParagraphID    string
	ParagraphIDs   string
	Resources      string
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
		idGenerator:  idgenerator.New(JobmanagerIDPrefix),
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

func (ex *JobmanagerExecutor) ModifyStatus(ctx context.Context, ID string, status string, message string, resources string) (err error) {
	var info JobmanagerInfo

	if len(message) > 4000 {
		message = message[0:4000]
	}

	info.ID = ID
	info.Status = status
	info.Message = message
	info.UpdateTime = time.Now().Format("2006-01-02 15:04:05")

	db := ex.db.WithContext(ctx)
	if err = db.Select("status", "message", "updatetime").Where("id = ? ", info.ID).Updates(info).Error; err != nil {
		return
	}

	if status == StatusFinish || status == StatusFailed {
		var rsjson map[string]string

		if tmperr := json.Unmarshal([]byte(resources), &rsjson); tmperr != nil {
			ex.logger.Warn().Msg("can't decode resources ").String("resource", resources).String("error", tmperr.Error()).String("job", ID).Fire()
		} else {
			if jar, ok := rsjson[FlinkJobJar]; ok == true {
				//TODO delete
				ex.logger.Info().Msg("delete jar").String("jar", jar).Fire()
			}
		}

		FreeEngine(ID)
		//TODO feedback schedule
	}

	return
}

func (ex *JobmanagerExecutor) RunJob(ctx context.Context, ID string, WorkspaceID string, NodeType string, Depends string, MainRun string) (rep jobpb.JobReply, err error) {
	var (
		noteID        string
		paragraphID   string
		resources     string
		paragraphJson map[string]string
		watchInfo     JobWatchInfo
	)

	rep, noteID, paragraphID, resources, err = ex.RunJobUtile(ctx, ID, WorkspaceID, NodeType, Depends, MainRun)
	if err != nil && noteID != "" {
		_ = ex.httpClient.DeleteNote(noteID) // every job id is different, delete it
		return
	}

	watchInfo.ID = ID
	watchInfo.NoteID = noteID
	watchInfo.ParagraphIDs = paragraphID
	watchInfo.Resources = resources
	ex.watchChan <- watchInfo

	_ = json.Unmarshal([]byte(paragraphID), &paragraphJson)

	rep.Status = StatusRunning
	rep.Message = "job running"

	// conf
	if err = ex.httpClient.RunParagraphSync(noteID, paragraphJson[JobConf]); err != nil {
		return
	}

	// depend
	if err = ex.httpClient.RunParagraphSync(noteID, paragraphJson[JobDepends]); err != nil {
		return
	}

	// main fun
	if err = ex.httpClient.RunParagraphAsync(noteID, paragraphJson[JobMainrun]); err != nil {
		return
	}

	return
}

func (ex *JobmanagerExecutor) RunJobUtile(ctx context.Context, ID string, WorkspaceID string, NodeType string, Depends string, MainRun string) (rep jobpb.JobReply, noteID string, paragraphID string, resources string, err error) {
	var (
		info             JobmanagerInfo
		zeplinConf       string
		zeplinDepends    string
		zeplinMainRun    string
		engineOptionJson map[string]string
		paragraph        map[string]string
		dependsJson      map[string]string
	)

	info.ID = ID
	info.CreateTime = time.Now().Format("2006-01-02 15:04:05")
	info.UpdateTime = info.CreateTime
	info.Status = StatusRunning
	info.Message = StatusRunning
	info.NoteID, err = ex.httpClient.CreateNote(ID)
	noteID = info.NoteID
	if err != nil {
		rep.Status = StatusFailed
		rep.Message = err.Error()
		return
	}
	paragraph = make(map[string]string)

	if err = json.Unmarshal([]byte(Depends), &dependsJson); err != nil {
		return
	}

	engineOptionJson = make(map[string]string)
	engineOptionJson[dependParallelism] = dependsJson[dependParallelism]
	engineOptionJson[dependJobCpu] = dependsJson[dependJobCpu]
	engineOptionJson[dependJobMem] = dependsJson[dependJobMem]
	engineOptionJson[dependTaskCpu] = dependsJson[dependTaskCpu]
	engineOptionJson[dependTaskMem] = dependsJson[dependTaskMem]
	engineOptionJson[dependTaskNum] = dependsJson[dependTaskNum]

	engineOptionByte, _ := json.Marshal(engineOptionJson)
	engineType, engineHost, enginePort, _, tmperr := GetEngine(WorkspaceID, ID, string(engineOptionByte))
	if tmperr != nil {
		err = tmperr
		return
	}

	// conf
	if engineType == "Flink" {
		zeplinConf = GenerateFlinkConf(ex.httpClient.ZeppelinFlinkHome, ex.httpClient.ZeppelinFlinkExecuteJars, engineHost, enginePort, NodeType)
	}
	paragraph[JobConf], err = ex.httpClient.CreateParagraph(info.NoteID, 0, JobConf, zeplinConf)
	if err != nil {
		rep.Status = StatusFailed
		rep.Message = err.Error()
		return
	}

	// depend and mainrun text
	if engineType == "Flink" {
		zeplinDepends, zeplinMainRun, resources, err = GenerateFlinkJob(ex.sourceClient, ex.httpClient.ZeppelinFlinkHome, engineHost+":"+enginePort, NodeType, Depends, MainRun)
	}
	if err != nil {
		rep.Status = StatusFailed
		rep.Message = err.Error()
		return
	}
	info.Resources = resources

	paragraph[JobDepends], err = ex.httpClient.CreateParagraph(info.NoteID, 1, JobDepends, zeplinDepends)
	if err != nil {
		rep.Status = StatusFailed
		rep.Message = err.Error()
		return
	}

	paragraph[JobMainrun], err = ex.httpClient.CreateParagraph(info.NoteID, 2, JobMainrun, zeplinMainRun)
	if err != nil {
		rep.Status = StatusFailed
		rep.Message = err.Error()
		return
	}

	paragraphJson, _ := json.Marshal(paragraph)
	info.Paragraph = string(paragraphJson)
	paragraphID = info.Paragraph

	rep.Status = info.Status
	rep.Message = info.Message

	db := ex.db.WithContext(ctx)
	err = db.Create(info).Error
	return
}

func GetNextParagraphID(job jobQueueType) (r jobQueueType) {
	order := make(map[int]string)
	order[0] = JobConf
	order[1] = JobDepends
	order[2] = JobMainrun

	r = job

	if job.ParagraphIndex == 2 {
		r.RunEnd = true
	} else {
		var paragraphJson map[string]string
		_ = json.Unmarshal([]byte(r.ParagraphIDs), &paragraphJson)

		r.ParagraphIndex = job.ParagraphIndex + 1
		r.ParagraphID = paragraphJson[order[r.ParagraphIndex]]
	}
	return
}

func InitJobInfo(watchInfo JobWatchInfo) (job jobQueueType) {
	var paragraphJson map[string]string
	_ = json.Unmarshal([]byte(watchInfo.ParagraphIDs), &paragraphJson)

	job.ID = watchInfo.ID
	job.NoteID = watchInfo.NoteID
	job.ParagraphIndex = 0
	job.ParagraphID = paragraphJson[JobConf]
	job.ParagraphIDs = watchInfo.ParagraphIDs
	job.RunEnd = false
	job.Resources = watchInfo.Resources

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
					if status, err = ex.httpClient.GetParagraphStatus(job.NoteID, job.ParagraphID); err != nil {
						ex.logger.Error().Msg("can't get this paragraph status").String("noteid", job.NoteID).String("paragraphid", job.ParagraphID).Fire()
						break
					}
					if status == ParagraphFinish {
						job = GetNextParagraphID(job)
						if job.RunEnd == true {
							if err = ex.ModifyStatus(ctx, job.ID, StatusFinish, JobSuccess, job.Resources); err != nil {
								ex.logger.Error().Msg("can't change the job status to finish").String("jobid", job.ID).Fire()
								break
							}

							if err = ex.httpClient.DeleteNote(job.NoteID); err != nil {
								ex.logger.Error().Msg("can't delete the job note").String("jobid", job.ID).Fire()
								break
							}
							delete(jobQueue, id)
							break
						}
						jobQueue[id] = job
						if err = ex.ModifyStatus(ctx, job.ID, StatusRunning, job.ParagraphID+" is running", job.Resources); err != nil {
							ex.logger.Error().Msg("can't change the job status to running").String("jobid", job.ID).String("paragraphid", job.ParagraphID).Fire()
							break
						}
					} else if status == ParagraphError {
						var joberrmsg string

						if joberrmsg, err = ex.httpClient.GetParagraphErrorMsg(job.NoteID, job.ParagraphID); err != nil {
							ex.logger.Error().Msg("can't get this paragraph info for a error paragraph").String("noteid", job.NoteID).String("paragraphid", job.ParagraphID).String("error msg", err.Error()).Fire()
							joberrmsg = "get error message failed"
						}

						if err = ex.ModifyStatus(ctx, job.ID, StatusFailed, joberrmsg, job.Resources); err != nil {
							ex.logger.Error().Msg("can't change the job status to failed").String("jobid", job.ID).Fire()
							break
						}

						if err = ex.httpClient.DeleteNote(job.NoteID); err != nil {
							ex.logger.Error().Msg("can't delete the job note").String("jobid", job.ID).Fire()
							break
						}
						delete(jobQueue, id)
						break
					} else if status == ParagraphAbort {
						if err = ex.ModifyStatus(ctx, job.ID, StatusFinish, JobAbort, job.Resources); err != nil {
							ex.logger.Error().Msg("can't change the job status to finish(abort)").String("jobid", job.ID).Fire()
							break
						}

						if err = ex.httpClient.DeleteNote(job.NoteID); err != nil {
							ex.logger.Error().Msg("can't delete the job note").String("jobid", job.ID).Fire()
							break
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
	var jobs []JobmanagerInfo

	db := ex.db.WithContext(ctx)
	if err := db.Table(JobmanagerTableName).Select("id, noteid, paragraph,resources").Where("status = '" + StatusRunning + "'").Scan(&jobs).Error; err != nil {
		ex.logger.Error().Msg("can't scan jobmanager table for pickup alone job").Fire()
		return
	}

	for _, job := range jobs {
		var watchInfo JobWatchInfo

		watchInfo.ID = job.ID
		watchInfo.NoteID = job.NoteID
		watchInfo.ParagraphIDs = job.Paragraph
		watchInfo.Resources = job.Resources
		ex.logger.Info().Msg("pickup alone job").String("jobid", job.ID).Fire()

		ex.watchChan <- watchInfo
	}

	return
}

func (ex *JobmanagerExecutor) GetJobStatus(ctx context.Context, ID string) (rep jobpb.JobReply, err error) {
	job, tmperr := ex.GetJobInfo(ctx, ID)
	ex.logger.Warn().Any("", job).Fire()

	if tmperr != nil {
		err = tmperr
		return
	}

	rep.Status = job.Status
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
	return
}
