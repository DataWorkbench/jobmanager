package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/jobdevpb"
	"github.com/DataWorkbench/gproto/pkg/jobpb"
	"github.com/DataWorkbench/gproto/pkg/jobwpb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/zepspb"
	"google.golang.org/grpc"

	"gorm.io/gorm"
)

type JobWatcherClient struct {
	client jobwpb.JobwatcherClient
	ctx    context.Context
}

func NewJobWatcherClient(serverAddr string) (c JobWatcherClient, err error) {
	var conn *grpc.ClientConn

	ctx := glog.WithContext(context.Background(), glog.NewDefault())
	conn, err = grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address:      serverAddr,
		LogLevel:     2,
		LogVerbosity: 99,
	})
	if err != nil {
		return
	}

	c.client = jobwpb.NewJobwatcherClient(conn)

	ln := glog.NewDefault().Clone()
	reqId, _ := idgenerator.New("").Take()
	ln.WithFields().AddString("rid", reqId)

	c.ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)

	return c, nil
}

type ZeppelinScaleClient struct {
	client zepspb.ZeppelinScaleClient
	ctx    context.Context
}

func NewZeppelinScaleClient(serverAddr string) (c ZeppelinScaleClient, err error) {
	var conn *grpc.ClientConn

	ctx := glog.WithContext(context.Background(), glog.NewDefault())
	conn, err = grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address:      serverAddr,
		LogLevel:     2,
		LogVerbosity: 99,
	})
	if err != nil {
		return
	}

	c.client = zepspb.NewZeppelinScaleClient(conn)

	ln := glog.NewDefault().Clone()
	reqId, _ := idgenerator.New("").Take()
	ln.WithFields().AddString("rid", reqId)

	c.ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)

	return c, nil
}

type JobmanagerExecutor struct {
	db                  *gorm.DB
	idGenerator         *idgenerator.IDGenerator
	jobDevClient        constants.JobdevClient
	jobWatcherClient    JobWatcherClient
	zeppelinScaleClient ZeppelinScaleClient
	ctx                 context.Context
	logger              *glog.Logger
}

func NewJobManagerExecutor(db *gorm.DB, job_client constants.JobdevClient, ictx context.Context, logger *glog.Logger, watcher_client JobWatcherClient, zeppelinscale_client ZeppelinScaleClient) *JobmanagerExecutor {
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

func (ex *JobmanagerExecutor) RunJob(ctx context.Context, ID string, WorkspaceID string, EngineID string, EngineType string, JobInfo string) (rep jobpb.JobReply, err error) {
	if EngineType == constants.ServerTypeFlink {
		var (
			job             constants.FlinkNode
			req             jobdevpb.JobParserRequest
			resp            *jobdevpb.JobElement
			flinkJobElement constants.JobElementFlink
			info            constants.JobmanagerInfo
			Pa              constants.FlinkParagraphsInfo
			httpClient      constants.HttpClient
			empty           model.EmptyStruct
		)

		defer func() {
			if err != nil {
				if tmp_err := constants.FreeJobResources(flinkJobElement.Resources, EngineType, ex.logger, httpClient, ex.jobDevClient); tmp_err != nil {
					ex.logger.Warn().String("can't delete jar", flinkJobElement.Resources.Jar).String("can't FreeEngine", flinkJobElement.Resources.JobID).Fire()
				}
				if info.NoteID != "" {
					_ = httpClient.DeleteNote(info.NoteID)
				}

				rep.Status = constants.StatusFailed
				rep.Message = fmt.Sprint(err)
			}
		}()

		zeppelinServer, tmperr := ex.zeppelinScaleClient.client.GetZeppelinAddress(ex.zeppelinScaleClient.ctx, &empty)
		if tmperr != nil {
			err = tmperr
			return
		}
		httpClient = constants.NewHttpClient(zeppelinServer.ServerAddress)

		if err = json.Unmarshal([]byte(JobInfo), &job); err != nil {
			return
		}

		req.ID = ID
		req.WorkspaceID = WorkspaceID
		req.EngineID = EngineID
		req.EngineType = EngineType
		req.JobInfo = JobInfo

		resp, err = ex.jobDevClient.Client.JobParser(ex.jobDevClient.Ctx, &req)
		if err != nil {
			return
		}

		if err = json.Unmarshal([]byte(resp.JobElement), &flinkJobElement); err != nil {
			return
		}

		if job.Command == constants.PreviewCommand {
			rep.Status = constants.StatusFinish
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
			info.EngineType = constants.ServerTypeFlink
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

			Pa.FuncScala, err = httpClient.CreateParagraph(info.NoteID, 2, "FuncScala", flinkJobElement.ZeppelinFuncScala)
			if err != nil {
				return
			}
			if flinkJobElement.ZeppelinFuncScala == "" {
				Pa.FuncScala = ""
			}

			Pa.MainRun, err = httpClient.CreateParagraph(info.NoteID, 3, "JobMainrun", flinkJobElement.ZeppelinMainRun)
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

			if job.Command == constants.RunCommand {
				var (
					watchInfo        constants.JobWatchInfo
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

				if Pa.FuncScala != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.FuncScala); err != nil {
						return
					}
				}

				if err = httpClient.RunParagraphAsync(info.NoteID, Pa.MainRun); err != nil {
					return
				}

				watchInfoByte, _ := json.Marshal(watchInfo)
				watchInfoRequest.JobInfo = string(watchInfoByte)
				_, err = ex.jobWatcherClient.client.WatchJob(ex.jobWatcherClient.ctx, &watchInfoRequest)
				if err != nil {
					return
				}

				rep.Status = constants.StatusRunning
				rep.Message = constants.JobRunning
				return
			} else if job.Command == constants.ExplainCommand {
				var (
					result string
				)

				if err = httpClient.RunParagraphSync(info.NoteID, Pa.Conf); err != nil {
					_ = constants.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
					rep.Status = constants.StatusFailed
					rep.Message = fmt.Sprint(err)
					return
				}

				if Pa.Depends != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.Depends); err != nil {
						_ = constants.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if Pa.FuncScala != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.FuncScala); err != nil {
						_ = constants.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if err = httpClient.RunParagraphSync(info.NoteID, Pa.MainRun); err != nil {
					_ = constants.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
					return
				}

				if result, err = httpClient.GetParagraphResultOutput(info.NoteID, Pa.MainRun); err != nil {
					rep.Status = constants.StatusFailed
				} else {
					rep.Status = constants.StatusFinish
				}
				rep.Message = result

				_ = constants.ModifyStatus(ctx, ID, rep.Status, result, flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
				if err = httpClient.DeleteNote(info.NoteID); err != nil {
					ex.logger.Error().Msg("can't delete the job note").String("jobid", ID).Fire()
				}

				return
			} else if job.Command == constants.SyntaxCheckCommand {
				var (
					result string
				)

				if err = httpClient.RunParagraphSync(info.NoteID, Pa.Conf); err != nil {
					_ = constants.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
					rep.Status = constants.StatusFailed
					rep.Message = fmt.Sprint(err)
					return
				}

				if Pa.Depends != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.Depends); err != nil {
						_ = constants.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if Pa.FuncScala != "" {
					if err = httpClient.RunParagraphSync(info.NoteID, Pa.FuncScala); err != nil {
						_ = constants.ModifyStatus(ctx, ID, constants.StatusFailed, fmt.Sprint(err), flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
						return
					}
				}

				if err = httpClient.RunParagraphSync(info.NoteID, Pa.MainRun); err != nil {
					rep.Status = constants.StatusFailed
					if result, err = httpClient.GetParagraphResultOutput(info.NoteID, Pa.MainRun); err != nil {
						rep.Message = "syntax check failed, can't get failed message."
					} else {
						result1 := result[strings.Index(result, "org.apache.flink.table.api."):strings.Index(result, "at org.apache.flink.table.")]
						rep.Message = result1[strings.Index(result1, ":")+1:]
					}
				} else {
					rep.Status = constants.StatusFinish
					rep.Message = "syntax check success"
				}

				_ = constants.ModifyStatus(ctx, ID, rep.Status, rep.Message, flinkJobElement.Resources, EngineType, ex.db, ex.logger, httpClient, ex.jobDevClient)
				if err = httpClient.DeleteNote(info.NoteID); err != nil {
					ex.logger.Error().Msg("can't delete the job note").String("jobid", ID).Fire()
				}

				return
			}
		}
	}

	return
}

func (ex *JobmanagerExecutor) GetJobStatus(ctx context.Context, ID string) (rep jobpb.JobReply, err error) {
	job, tmperr := ex.GetJobInfo(ctx, ID)

	if tmperr != nil {
		err = tmperr
		return
	}

	rep.Status = constants.StringStatusToInt32(job.Status)
	rep.Message = job.Message
	return
}

func (ex *JobmanagerExecutor) GetJobInfo(ctx context.Context, ID string) (job constants.JobmanagerInfo, err error) {
	db := ex.db.WithContext(ctx)
	err = db.Table(constants.JobmanagerTableName).Select("noteid, status,message,enginetype,zeppelinserver").Where("id = '" + ID + "'").Scan(&job).Error
	return
}

func (ex *JobmanagerExecutor) CancelJob(ctx context.Context, ID string) (err error) {
	job, tmperr := ex.GetJobInfo(ctx, ID)
	if tmperr != nil {
		err = tmperr
		return
	}
	ex.logger.Warn().Msg("user cancel job").String("id", ID).Any("", job).Fire() // if use cancel.  log is necessary
	httpClient := constants.NewHttpClient(job.ZeppelinServer)
	err = httpClient.StopAllParagraphs(job.NoteID)
	if job.EngineType == constants.ServerTypeFlink {
		//TODO savepoint at here
	}
	return
}

func (ex *JobmanagerExecutor) CancelAllJob(ctx context.Context, SpaceID string) (err error) {
	var (
		jobs []constants.JobmanagerInfo
	)

	db := ex.db.WithContext(ctx)
	if err = db.Table(constants.JobmanagerTableName).Select("id").Where("spaceid = '" + SpaceID + "' and status = '" + constants.StatusRunningString + "'").Scan(&jobs).Error; err != nil {
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
