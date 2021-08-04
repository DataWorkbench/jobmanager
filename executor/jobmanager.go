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
	"github.com/DataWorkbench/gproto/pkg/jobdevpb"
	"github.com/DataWorkbench/gproto/pkg/jobpb"
	"github.com/DataWorkbench/gproto/pkg/jobwpb"
	"github.com/DataWorkbench/gproto/pkg/model"
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
	jobWatcherClient    JobWatcherClient
	zeppelinScaleClient ZeppelinScaleClient
	ctx                 context.Context
	logger              *glog.Logger
}

func NewJobManagerExecutor(db *gorm.DB, job_client functions.JobdevClient, ictx context.Context, logger *glog.Logger, watcher_client JobWatcherClient, zeppelinscale_client ZeppelinScaleClient) *JobmanagerExecutor {
	ex := &JobmanagerExecutor{
		db:                  db,
		idGenerator:         idgenerator.New(constants.JobIDPrefix),
		jobDevClient:        job_client,
		ctx:                 ictx,
		logger:              logger,
		jobWatcherClient:    watcher_client,
		zeppelinScaleClient: zeppelinscale_client,
	}

	return ex
}

func (ex *JobmanagerExecutor) RunJob(ctx context.Context, ID string, WorkspaceID string, EngineID string, EngineType string, Command string, JobInfo string) (rep jobpb.JobReply, err error) {
	if EngineType == constants.EngineTypeFlink {
		var (
			job             constants.FlinkNode
			req             jobdevpb.JobParserRequest
			resp            *jobdevpb.JobElement
			flinkJobElement constants.JobElementFlink
			info            functions.JobmanagerInfo
			Pa              constants.FlinkParagraphsInfo
			httpClient      functions.HttpClient
			createRecord    bool
			empty           model.EmptyStruct
		)

		defer func() {
			if err != nil {
				if tmp_err := functions.FreeJobResources(ctx, flinkJobElement.Resources, EngineType, ex.logger, httpClient, ex.jobDevClient); tmp_err != nil {
					ex.logger.Warn().String("can't delete jar", flinkJobElement.Resources.Jar).String("can't FreeEngine", flinkJobElement.Resources.JobID).Fire()
				}
				if info.NoteID != "" {
					_ = httpClient.DeleteNote(info.NoteID)
				}

				rep.State = constants.StatusFailed
				rep.Message = fmt.Sprint(err)

				if createRecord == true {
					_ = functions.ModifyStatus(ctx, ID, rep.State, rep.Message, flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
				}

			}
		}()

		zeppelinServer, tmperr := ex.zeppelinScaleClient.client.GetZeppelinAddress(ctx, &empty)
		if tmperr != nil {
			err = tmperr
			return
		}
		httpClient = functions.NewHttpClient(zeppelinServer.ServerAddress)

		if err = json.Unmarshal([]byte(JobInfo), &job); err != nil {
			return
		}

		req.ID = ID
		req.WorkspaceID = WorkspaceID
		req.EngineID = EngineID
		req.EngineType = EngineType
		req.Command = Command
		req.JobInfo = JobInfo

		resp, err = ex.jobDevClient.Client.JobParser(ctx, &req)
		if err != nil {
			return
		}

		if err = json.Unmarshal([]byte(resp.JobElement), &flinkJobElement); err != nil {
			return
		}

		if Command == constants.PreviewCommand {
			rep.State = constants.StatusFinish
			if flinkJobElement.ZeppelinDepends != "" {
				rep.Message += flinkJobElement.ZeppelinDepends[strings.Index(flinkJobElement.ZeppelinDepends, "\n"):]
			}
			if flinkJobElement.ZeppelinMainRun != "" {
				rep.Message += flinkJobElement.ZeppelinMainRun[strings.Index(flinkJobElement.ZeppelinMainRun, "\n"):]
			}
			return
		} else {
			resourcesByte, _ := json.Marshal(flinkJobElement.Resources)
			info.Resources = string(resourcesByte)
			info.ID = ID
			info.CreateTime = time.Now().Format("2006-01-02 15:04:05")
			info.UpdateTime = info.CreateTime
			info.SpaceID = WorkspaceID
			info.NoteID, err = httpClient.CreateNote(ID)
			info.EngineType = constants.EngineTypeFlink
			info.ZeppelinServer = zeppelinServer.ServerAddress
			if err != nil {
				return
			}

			Pa.Conf, err = httpClient.CreateParagraph(info.NoteID, 0, "JobConf", flinkJobElement.ZeppelinConf)
			if err != nil {
				return
			}

			Pa.Depends, err = httpClient.CreateParagraph(info.NoteID, 1, "JobDepends", flinkJobElement.ZeppelinDepends)
			if err != nil {
				return
			}
			if flinkJobElement.ZeppelinDepends == "" {
				Pa.Depends = ""
			}

			Pa.ScalaUDF, err = httpClient.CreateParagraph(info.NoteID, 2, "ScalaUDF", flinkJobElement.ZeppelinScalaUDF)
			if err != nil {
				return
			}
			if flinkJobElement.ZeppelinScalaUDF == "" {
				Pa.ScalaUDF = ""
			}

			Pa.PythonUDF, err = httpClient.CreateParagraph(info.NoteID, 3, "PythonUDF", flinkJobElement.ZeppelinPythonUDF)
			if err != nil {
				return
			}

			if flinkJobElement.ZeppelinPythonUDF == "" {
				Pa.PythonUDF = ""
			}

			Pa.MainRun, err = httpClient.CreateParagraph(info.NoteID, 4, "JobMainrun", flinkJobElement.ZeppelinMainRun)
			if err != nil {
				return
			}

			PaByte, _ := json.Marshal(Pa)
			info.Paragraph = string(PaByte)
			info.Status = constants.StatusRunningString
			info.Message = constants.JobRunning
			db := ex.db.WithContext(ctx)
			err = db.Create(info).Error
			if err != nil {
				return
			}
			createRecord = true

			if Command == constants.RunCommand {
				var (
					watchInfo        functions.JobWatchInfo
					watchInfoRequest jobwpb.WatchJobRequest
				)

				watchInfo.FlinkParagraphIDs = Pa
				watchInfo.EngineType = EngineType
				watchInfo.ID = info.ID
				watchInfo.NoteID = info.NoteID
				watchInfo.FlinkResources = flinkJobElement.Resources
				watchInfo.ServerAddr = zeppelinServer.ServerAddress

				if err = httpClient.RunParagraphSync(info.NoteID, Pa.Conf); err != nil {
					return
				}

				if Pa.Depends != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.Depends); err != nil {
						return
					}
				}

				if Pa.ScalaUDF != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.ScalaUDF); err != nil {
						return
					}
				}

				if Pa.PythonUDF != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.PythonUDF); err != nil {
						return
					}
				}

				if err = httpClient.RunParagraphAsync(info.NoteID, Pa.MainRun); err != nil {
					return
				}

				watchInfoByte, _ := json.Marshal(watchInfo)
				watchInfoRequest.JobInfo = string(watchInfoByte)
				_, err = ex.jobWatcherClient.client.WatchJob(ctx, &watchInfoRequest)
				if err != nil {
					return
				}

				rep.State = constants.StatusRunning
				rep.Message = constants.JobRunning
				return
			} else if Command == constants.ExplainCommand {
				var (
					result string
				)

				if err = httpClient.RunParagraphSync(info.NoteID, Pa.Conf); err != nil {
					_ = functions.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
					rep.State = constants.StatusFailed
					rep.Message = fmt.Sprint(err)
					return
				}

				if Pa.Depends != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.Depends); err != nil {
						_ = functions.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if Pa.ScalaUDF != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.ScalaUDF); err != nil {
						_ = functions.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if Pa.PythonUDF != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.PythonUDF); err != nil {
						_ = functions.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if err = httpClient.RunParagraphSync(info.NoteID, Pa.MainRun); err != nil {
					_ = functions.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
					return
				}

				if result, err = httpClient.GetParagraphResultOutput(info.NoteID, Pa.MainRun); err != nil {
					rep.State = constants.StatusFailed
				} else {
					rep.State = constants.StatusFinish
				}
				rep.Message = result

				_ = functions.ModifyStatus(ctx, ID, rep.State, result, flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
				if err = httpClient.DeleteNote(info.NoteID); err != nil {
					ex.logger.Error().Msg("can't delete the job note").String("jobid", ID).Fire()
				}

				return
			} else if Command == constants.SyntaxCheckCommand {
				var (
					result string
				)

				if err = httpClient.RunParagraphSync(info.NoteID, Pa.Conf); err != nil {
					_ = functions.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
					rep.State = constants.StatusFailed
					rep.Message = fmt.Sprint(err)
					return
				}

				if Pa.Depends != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.Depends); err != nil {
						_ = functions.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if Pa.ScalaUDF != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.ScalaUDF); err != nil {
						_ = functions.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if Pa.PythonUDF != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.PythonUDF); err != nil {
						_ = functions.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if err = httpClient.RunParagraphSync(info.NoteID, Pa.MainRun); err != nil {
					rep.State = constants.StatusFailed
					if result, err = httpClient.GetParagraphResultOutput(info.NoteID, Pa.MainRun); err != nil {
						rep.Message = "syntax check failed, can't get failed message."
					} else {
						result1 := result[strings.Index(result, "org.apache.flink.table.api."):strings.Index(result, "at org.apache.flink.table.")]
						rep.Message = result1[strings.Index(result1, ":")+1:]
					}
				} else {
					rep.State = constants.StatusFinish
					rep.Message = "syntax check success"
				}

				_ = functions.ModifyStatus(ctx, ID, rep.State, rep.Message, flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
				if err = httpClient.DeleteNote(info.NoteID); err != nil {
					ex.logger.Error().Msg("can't delete the job note").String("jobid", ID).Fire()
				}

				return
			}
		}
	}

	return
}

func (ex *JobmanagerExecutor) GetJobState(ctx context.Context, ID string) (rep jobpb.JobReply, err error) {
	job, tmperr := ex.GetJobInfo(ctx, ID)

	if tmperr != nil {
		err = tmperr
		return
	}

	rep.State = functions.StringStatusToInt32(job.Status)
	rep.Message = job.Message
	return
}

func (ex *JobmanagerExecutor) GetJobInfo(ctx context.Context, ID string) (job functions.JobmanagerInfo, err error) {
	db := ex.db.WithContext(ctx)
	err = db.Table(functions.JobmanagerTableName).Select("noteid, status,message,enginetype,zeppelinserver").Where("id = '" + ID + "'").Scan(&job).Error
	return
}

func (ex *JobmanagerExecutor) CancelJob(ctx context.Context, ID string) (err error) {
	job, tmperr := ex.GetJobInfo(ctx, ID)
	if tmperr != nil {
		err = tmperr
		return
	}
	ex.logger.Warn().Msg("user cancel job").String("id", ID).Any("", job).Fire() // if use cancel.  log is necessary
	httpClient := functions.NewHttpClient(job.ZeppelinServer)
	err = httpClient.StopAllParagraphs(job.NoteID)
	if job.EngineType == constants.EngineTypeFlink {
		//TODO savepoint at here
	}
	return
}

func (ex *JobmanagerExecutor) CancelAllJob(ctx context.Context, SpaceID string) (err error) {
	var (
		jobs []functions.JobmanagerInfo
	)

	db := ex.db.WithContext(ctx)
	if err = db.Table(functions.JobmanagerTableName).Select("id").Where("spaceid = '" + SpaceID + "' and status = '" + constants.StatusRunningString + "'").Scan(&jobs).Error; err != nil {
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
