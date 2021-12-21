package executor

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"gorm.io/gorm/clause"
	"math/big"
	"strings"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/functions"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/jobwpb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"

	"gorm.io/gorm"
)

type JobWatcherClient struct {
	client jobwpb.JobwatcherClient
}

func NewJobWatcherClient(conn *grpcwrap.ClientConn) (c JobWatcherClient, err error) {
	c.client = jobwpb.NewJobwatcherClient(conn)
	return c, nil
}

type JobmanagerExecutor struct {
	db               *gorm.DB
	idGenerator      *idgenerator.IDGenerator
	jobDevClient     functions.JobdevClient
	engineClient     EngineClient
	jobWatcherClient JobWatcherClient
	ctx              context.Context
	logger           *glog.Logger
	zeppelin_address string
}

func NewJobManagerExecutor(db *gorm.DB, eClient EngineClient, job_client functions.JobdevClient, ictx context.Context, logger *glog.Logger, watcher_client JobWatcherClient, zeppelin_address string) *JobmanagerExecutor {
	ex := &JobmanagerExecutor{
		db:               db,
		idGenerator:      idgenerator.New(constants.JobIDPrefix),
		jobDevClient:     job_client,
		ctx:              ictx,
		logger:           logger,
		jobWatcherClient: watcher_client,
		engineClient:     eClient,
		zeppelin_address: zeppelin_address,
	}

	return ex
}

func CreateRandomString(len int) string {
	var container string
	var str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	b := bytes.NewBufferString(str)
	length := b.Len()
	bigInt := big.NewInt(int64(length))
	for i := 0; i < len; i++ {
		randomInt, _ := rand.Int(rand.Reader, bigInt)
		container += string(str[randomInt.Int64()])
	}
	return container
}

func (ex *JobmanagerExecutor) RunJob(ctx context.Context, jobInfo *request.JobInfo, cmd string) (jobState response.JobState, err error) {
	var (
		zeppelinClient functions.HttpClient
		jobParserResp  *response.JobParser
		Pa             constants.FlinkParagraphsInfo
		noteID         string
		PaID           string
	)

	defer func() {
		if err != nil {
			if jobParserResp != nil && jobParserResp.Resources != nil {
				_ = functions.FreeJobResources(ctx, *jobParserResp.Resources, ex.logger, zeppelinClient, ex.jobDevClient)
			}
			jobState.State = model.StreamJobInst_Failed
			if noteID != "" {
				if jobState.Message == "" && PaID != "" {
					output, err1 := zeppelinClient.GetParagraphResultOutput(noteID, PaID)
					if err1 != nil {
						jobState.Message = fmt.Sprint(err1)
					} else {
						jobState.Message = output
					}
				}
				jobState.Message += fmt.Sprint(err)

				ex.logger.Warn().Msg("run job error").String("jobid", jobInfo.JobId).Any("errmsg", jobState.Message).Fire()
				_ = zeppelinClient.DeleteNote(noteID)
			}
		}
	}()

	zeppelinAddress := ex.zeppelin_address
	zeppelinClient = functions.NewHttpClient(zeppelinAddress)

	jobParserResp, err = ex.jobDevClient.Client.JobParser(ctx, &request.JobParser{Job: jobInfo, Command: cmd})
	if err != nil {
		jobParserResp = nil
		return
	}

	if cmd == constants.JobCommandPreview {
		jobState.State = model.StreamJobInst_Succeed
		if jobParserResp.ZeppelinDepends != "" {
			jobState.Message += jobParserResp.ZeppelinDepends[strings.Index(jobParserResp.ZeppelinDepends, "\n"):]
		}
		if jobParserResp.ZeppelinMainRun != "" {
			jobState.Message += jobParserResp.ZeppelinMainRun[strings.Index(jobParserResp.ZeppelinMainRun, "\n"):]
		}
		return
	} else {
		noteName := jobInfo.JobId
		if cmd == constants.JobCommandSyntax {
			noteName = "syx-" + CreateRandomString(16)
		}
		noteID, err = zeppelinClient.CreateNote(noteName)
		ex.logger.Info().Msg("note id is " + noteID)
		if err != nil {
			return
		}

		Pa.Conf, err = zeppelinClient.CreateParagraph(noteID, 0, "JobConf", jobParserResp.ZeppelinConf)
		if err != nil {
			return
		}

		Pa.Depends, err = zeppelinClient.CreateParagraph(noteID, 1, "JobDepends", jobParserResp.ZeppelinDepends)
		if err != nil {
			return
		}
		if jobParserResp.ZeppelinDepends == "" {
			Pa.Depends = ""
		}

		Pa.ScalaUDF, err = zeppelinClient.CreateParagraph(noteID, 2, "ScalaUDF", jobParserResp.ZeppelinScalaUDF)
		if err != nil {
			return
		}
		if jobParserResp.ZeppelinScalaUDF == "" {
			Pa.ScalaUDF = ""
		}

		Pa.PythonUDF, err = zeppelinClient.CreateParagraph(noteID, 3, "PythonUDF", jobParserResp.ZeppelinPythonUDF)
		if err != nil {
			return
		}
		if jobParserResp.ZeppelinPythonUDF == "" {
			Pa.PythonUDF = ""
		}

		Pa.MainRun, err = zeppelinClient.CreateParagraph(noteID, 4, "JobMainrun", jobParserResp.ZeppelinMainRun)
		if err != nil {
			return
		}

		//TODO 这里需要任务具体类型
		if cmd == constants.JobCommandSyntax {
			var output string
			var outputJson []map[string]string

			if err = zeppelinClient.RunParagraphSync(noteID, Pa.MainRun); err != nil {
				ex.logger.Error().Msg("can't check syntax").String("sql", jobParserResp.ZeppelinMainRun).Any("", err).Fire()
				err = nil
			}

			if output, err = zeppelinClient.GetParagraphResultOutput(noteID, Pa.MainRun); err != nil {
				PaID = Pa.MainRun
				return
			}

			if err = json.Unmarshal([]byte(output), &outputJson); err != nil {
				ex.logger.Error().Msg("can't check syntax").String("sql", jobParserResp.ZeppelinMainRun).Any("can't Unmarshal output", err).Fire()
				err = nil
			}

			if jobInfo.Code.Type > 2 || outputJson[0]["data"][0] == '0' {
				jobState.State = model.StreamJobInst_Succeed
				jobState.Message = "job is running"
			} else {
				jobState.State = model.StreamJobInst_Failed
				jobState.Message = outputJson[0]["data"]
			}

			if err = zeppelinClient.DeleteNote(noteID); err != nil {
				ex.logger.Warn().Msg("can't delete the job note").String("jobid", jobInfo.JobId).Fire()
				err = nil
			}
			noteID = ""

			return
		} else if cmd == constants.JobCommandRun {
			var (
				info functions.JobmanagerInfo
				//watchInfoRequest jobwpb.WatchJobRequest
			)

			if err = zeppelinClient.RunParagraphSync(noteID, Pa.Conf); err != nil {
				PaID = Pa.Conf
				return
			}

			if Pa.Depends != "" {
				if err = zeppelinClient.RunParagraphSync(noteID, Pa.Depends); err != nil {
					PaID = Pa.Depends
					return
				}
			}

			if Pa.ScalaUDF != "" {
				if err = zeppelinClient.RunParagraphSync(noteID, Pa.ScalaUDF); err != nil {
					PaID = Pa.ScalaUDF
					return
				}
			}

			if Pa.PythonUDF != "" {
				if err = zeppelinClient.RunParagraphSync(noteID, Pa.PythonUDF); err != nil {
					PaID = Pa.PythonUDF
					return
				}
			}

			if err = zeppelinClient.RunParagraphAsync(noteID, Pa.MainRun); err != nil {
				PaID = Pa.MainRun
				return
			}
			info.JobID = jobInfo.JobId
			info.SpaceID = jobInfo.SpaceId
			info.NoteID = noteID
			info.Status = model.StreamJobInst_Running
			info.Message = constants.MessageRunning
			info.Created = time.Now().Unix()
			info.Updated = info.Created
			info.ZeppelinServer = zeppelinAddress
			PaByte, _ := json.Marshal(Pa)
			info.Paragraph = string(PaByte)
			if jobParserResp.Resources != nil {
				resourcesByte, _ := json.Marshal(jobParserResp.Resources)
				info.Resources = string(resourcesByte)
			}

			//watchInfo := functions.JobInfoToWatchInfo(info)
			//watchInfoByte, _ := json.Marshal(watchInfo)
			//watchInfoRequest.JobInfo = string(watchInfoByte)
			//_, err = ex.jobWatcherClient.client.WatchJob(ctx, &watchInfoRequest)
			//if err != nil {
			//	return
			//}

			//if err = ex.db.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(&info).Error;err!=nil{
			//	return
			//}
			if err = ex.db.WithContext(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "job_id"}},
				UpdateAll: true,
			}).Create(&info).Error; err != nil {
				return
			}

			jobState.State = model.StreamJobInst_Running
			jobState.Message = constants.MessageRunning

			return
		}
	}

	return
}

func (ex *JobmanagerExecutor) GetState(ctx context.Context, ID string) (jobState response.JobState, err error) {
	var jobInfo functions.JobmanagerInfo

	jobInfo, err = ex.GetJobInfo(ctx, ID)
	if err != nil {
		return
	}

	if jobInfo.Status == model.StreamJobInst_Running {
		job := functions.InitJobInfo(functions.JobInfoToWatchInfo(jobInfo))
		for true {
			retjob, _ := functions.GetZeppelinJobState(ctx, job, ex.logger, ex.db, ex.jobDevClient)
			if jobState.State == model.StreamJobInst_Retrying {
				if retjob.StatusFailedNum >= functions.MaxStatusFailedNum {
					jobState.State = model.StreamJobInst_Timeout
					jobState.Message = constants.MessageUnknowState
				} else {
					time.Sleep(time.Second)
					job = retjob
				}
				return
			} else {
				jobState = retjob.Watch.JobState
				return
			}
		}

	} else {
		jobState.State = jobInfo.Status
		jobState.Message = jobInfo.Message
	}

	return
}

func (ex *JobmanagerExecutor) GetJobInfo(ctx context.Context, ID string) (job functions.JobmanagerInfo, err error) {
	db := ex.db.WithContext(ctx)
	err = db.Table(functions.JobTableName).Select("*").Where("job_id = '" + ID + "'").Scan(&job).Error
	return
}

func (ex *JobmanagerExecutor) CancelJob(ctx context.Context, jobID string) (err error) {
	var jobInfo functions.JobmanagerInfo

	jobInfo, err = ex.GetJobInfo(ctx, jobID)
	if err != nil {
		return
	}

	ex.logger.Warn().Msg("user cancel job").String("jobid", jobID).Any("", jobInfo).Fire() // if use cancel.  log is necessary
	zeppelinClient := functions.NewHttpClient(jobInfo.ZeppelinServer)
	err = zeppelinClient.StopAllParagraphs(jobInfo.NoteID)
	if err != nil {
		return
	}

	if err = functions.ModifyState(ctx, jobInfo.JobID, model.StreamJobInst_Succeed, "user cancel job", ex.db); err != nil {
		ex.logger.Error().Msg("can't change the job status to terminated").String("jobid", jobInfo.JobID).Fire()
		return
	}

	if err = zeppelinClient.DeleteNote(jobInfo.NoteID); err != nil {
		ex.logger.Error().Msg("can't delete the note").String("noteid", jobInfo.NoteID).String("jobid", jobInfo.JobID).String("error msg", err.Error()).Fire()
		err = nil
	}

	_ = functions.FreeJobResources(ctx, functions.JobInfoToWatchInfo(jobInfo).FlinkResources, ex.logger, zeppelinClient, ex.jobDevClient)

	return
}

func (ex *JobmanagerExecutor) CancelAllJob(ctx context.Context, SpaceIDs []string) (err error) {
	db := ex.db.WithContext(ctx)
	for _, SpaceID := range SpaceIDs {
		var (
			jobs []functions.JobmanagerInfo
		)

		if err = db.Table(functions.JobTableName).Select("job_id").Where("space_id = ? and status = ? ", SpaceID, model.StreamJobInst_Running).Scan(&jobs).Error; err != nil {
			ex.logger.Error().Msg("can't scan jobmanager table for cancel all job").Fire()
			return
		}

		for _, job := range jobs {
			tmperr := ex.CancelJob(ctx, job.JobID)
			if tmperr == nil {
				ex.logger.Info().String("cancel all running jobid", job.JobID).Fire()
			} else {
				ex.logger.Error().String("cancel all running jobid", job.JobID).Fire()
			}
		}
	}

	return
}

func (ex *JobmanagerExecutor) NodeRelations(ctx context.Context) (resp *response.NodeRelations, err error) {
	resp, err = ex.jobDevClient.Client.NodeRelations(ctx, &model.EmptyStruct{})
	return
}
