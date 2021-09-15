package executor

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/DataWorkbench/gproto/pkg/zepspb"

	"gorm.io/gorm"
)

type JobWatcherClient struct {
	client jobwpb.JobwatcherClient
}

func NewJobWatcherClient(conn *grpcwrap.ClientConn) (c JobWatcherClient, err error) {
	c.client = jobwpb.NewJobwatcherClient(conn)
	return c, nil
}

type ZeppelinScaleClient struct {
	client zepspb.ZeppelinScaleClient
}

func NewZeppelinScaleClient(conn *grpcwrap.ClientConn) (c ZeppelinScaleClient, err error) {
	c.client = zepspb.NewZeppelinScaleClient(conn)
	return c, nil
}

type JobmanagerExecutor struct {
	db                  *gorm.DB
	idGenerator         *idgenerator.IDGenerator
	jobDevClient        functions.JobdevClient
	engineClient        EngineClient
	jobWatcherClient    JobWatcherClient
	zeppelinScaleClient ZeppelinScaleClient
	ctx                 context.Context
	logger              *glog.Logger
}

func NewJobManagerExecutor(db *gorm.DB, eClient EngineClient, job_client functions.JobdevClient, ictx context.Context, logger *glog.Logger, watcher_client JobWatcherClient, zeppelinscale_client ZeppelinScaleClient) *JobmanagerExecutor {
	ex := &JobmanagerExecutor{
		db:                  db,
		idGenerator:         idgenerator.New(constants.JobIDPrefix),
		jobDevClient:        job_client,
		ctx:                 ictx,
		logger:              logger,
		jobWatcherClient:    watcher_client,
		zeppelinScaleClient: zeppelinscale_client,
		engineClient:        eClient,
	}

	return ex
}

func (ex *JobmanagerExecutor) RunJob(ctx context.Context, jobInfo *request.JobInfo, cmd string) (jobState response.JobState, err error) {
	var (
		zeppelinAddress *response.ZeppelinAddress
		zeppelinClient  functions.HttpClient
		jobParserResp   *response.JobParser
		Pa              constants.FlinkParagraphsInfo
		noteID          string
		PaID            string
	)

	defer func() {
		if err != nil {
			if jobParserResp.Resources != nil {
				_ = functions.FreeJobResources(ctx, *jobParserResp.Resources, ex.logger, zeppelinClient, ex.jobDevClient)
			}

			if jobState.Message == "" {
				jobState.State = int32(constants.StatusFailed)
				if noteID != "" && PaID != "" {
					var (
						output string
						errerr error
					)

					output, errerr = zeppelinClient.GetParagraphResultOutput(noteID, PaID)
					if errerr != nil {
						jobState.Message = fmt.Sprint(errerr) + "," + fmt.Sprint(err)
					} else {
						jobState.Message = output + "," + fmt.Sprint(err)
					}
				} else {
					jobState.Message = fmt.Sprint(err)
				}
			}

			if noteID != "" {
				_ = zeppelinClient.DeleteNote(noteID)
			}
		}
	}()

	zeppelinAddress, err = ex.zeppelinScaleClient.client.GetZeppelinAddress(ctx, &model.EmptyStruct{})
	if err != nil {
		return
	}
	zeppelinClient = functions.NewHttpClient(zeppelinAddress.ServerAddress)

	jobParserResp, err = ex.jobDevClient.Client.JobParser(ctx, &request.JobParser{Job: jobInfo, Command: cmd})
	if err != nil {
		return
	}

	if cmd == constants.JobCommandPreview {
		jobState.State = int32(constants.StatusFinish)
		if jobParserResp.ZeppelinDepends != "" {
			jobState.Message += jobParserResp.ZeppelinDepends[strings.Index(jobParserResp.ZeppelinDepends, "\n"):]
		}
		if jobParserResp.ZeppelinMainRun != "" {
			jobState.Message += jobParserResp.ZeppelinMainRun[strings.Index(jobParserResp.ZeppelinMainRun, "\n"):]
		}
		return
	} else {
		noteID, err = zeppelinClient.CreateNote(jobInfo.JobID)
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

		if cmd == constants.JobCommandExplain {
			if err = zeppelinClient.RunParagraphSync(noteID, Pa.MainRun); err != nil {
				PaID = Pa.MainRun
				return
			}

			explainOutput := ""
			if explainOutput, err = zeppelinClient.GetParagraphResultOutput(noteID, Pa.MainRun); err != nil {
				PaID = Pa.MainRun
				return
			}
			jobState.State = int32(constants.StatusFinish)
			jobState.Message = explainOutput

			if err = zeppelinClient.DeleteNote(noteID); err != nil {
				ex.logger.Warn().Msg("can't delete the job note").String("jobid", jobInfo.JobID).Fire()
				err = nil
			}
			noteID = ""

			return
		} else if cmd == constants.JobCommandSyntax {
			if err = zeppelinClient.RunParagraphSync(noteID, Pa.MainRun); err != nil {
				var output string

				if output, err = zeppelinClient.GetParagraphResultOutput(noteID, Pa.MainRun); err != nil {
					PaID = Pa.MainRun
					return
				}
				result := output[strings.Index(output, "org.apache.flink.table.api."):strings.Index(output, "at org.apache.flink.table.")]
				jobState.Message = result[strings.Index(result, ":")+1:]
				jobState.State = int32(constants.StatusFailed)
				err = nil
			} else {
				jobState.State = int32(constants.StatusFinish)
				jobState.Message = constants.MessageFinish
			}

			if err = zeppelinClient.DeleteNote(noteID); err != nil {
				ex.logger.Warn().Msg("can't delete the job note").String("jobid", jobInfo.JobID).Fire()
				err = nil
			}
			noteID = ""

			return
		} else if cmd == constants.JobCommandRun {
			var (
				info functions.JobmanagerInfo
				//watchInfoRequest jobwpb.WatchJobRequest
			)
			if err = zeppelinClient.RunParagraphAsync(noteID, Pa.MainRun); err != nil {
				PaID = Pa.MainRun
				return
			}
			info.JobID = jobInfo.JobID
			info.SpaceID = jobInfo.SpaceID
			info.NoteID = noteID
			info.Status = constants.StatusRunningString
			info.Message = constants.MessageRunning
			info.CreateTime = time.Now().Format("2006-01-02 15:04:05")
			info.UpdateTime = info.CreateTime
			info.ZeppelinServer = zeppelinAddress.ServerAddress
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

			db := ex.db.WithContext(ctx)
			err = db.Create(info).Error
			if err != nil {
				return
			}

			jobState.State = int32(constants.StatusRunning)
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

	if jobInfo.Status == constants.StatusRunningString {
		job := functions.InitJobInfo(functions.JobInfoToWatchInfo(jobInfo))
		for true {
			retjob, _ := functions.GetZeppelinJobState(ctx, job, ex.logger, ex.db, ex.jobDevClient)
			if retjob.StatusFailedNum == functions.MaxStatusFailedNum {
				jobState.State = int32(constants.StatusRunning)
				jobState.Message = constants.MessageUnknowState
				return
			} else if retjob.Watch.JobState.State != int32(constants.StatusRunning) {
				jobState = retjob.Watch.JobState
				return
			} else {
				time.Sleep(time.Second)
				job = retjob
			}
		}

	} else {
		jobState.State = functions.StringStatusToInt32(jobInfo.Status)
		jobState.Message = jobInfo.Message
	}

	return
}

func (ex *JobmanagerExecutor) GetJobInfo(ctx context.Context, ID string) (job functions.JobmanagerInfo, err error) {
	db := ex.db.WithContext(ctx)
	err = db.Table(functions.JobTableName).Select("*").Where("jobid = '" + ID + "'").Scan(&job).Error
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

	if err = functions.ModifyState(ctx, jobInfo.JobID, int32(constants.StatusTerminated), "user cancel job", ex.db); err != nil {
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

		if err = db.Table(functions.JobTableName).Select("jobid").Where("spaceid = ? and status = ? ", SpaceID, constants.StatusRunningString).Scan(&jobs).Error; err != nil {
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

func (ex *JobmanagerExecutor) CleanJob(ctx context.Context, jobID string) (err error) {
	//_, err = ex.engineClient.client.Delete(ctx, &enginepb.DeleteFlinkRequest{Name: jobID})
	//if err != nil {
	//	return
	//}
	if err = functions.ModifyState(ctx, jobID, int32(constants.StatusFinish), "finish and clean engine.", ex.db); err != nil {
		ex.logger.Error().Msg("can't change the job status to terminated").String("jobid", jobID).Fire()
		return
	}
	return
}
