package flink

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/gproto/pkg/model"
	"strings"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type JarExecutor struct {
	*BaseExecutor
	ctx context.Context
}

func NewJarExecutor(bm *BaseExecutor, ctx context.Context) *JarExecutor {
	return &JarExecutor{
		BaseExecutor: bm,
		ctx:          ctx,
	}
}

func (jarExec *JarExecutor) Run(ctx context.Context, info *request.RunJob) (*zeppelin.ParagraphResult, error) {
	var result *zeppelin.ParagraphResult
	var noteId string
	var err error
	defer func() {
		//TODO 如果notebook已经被创建，删除notebook
		if noteId != "" && len(noteId) > 0 {
			_ = jarExec.zeppelinClient.DeleteNote(noteId)
		}
	}()

	result, err = jarExec.preCheck(ctx, info.InstanceId)
	if err != nil {
		return nil, err
	} else if result != nil {
		return result, nil
	}

	jar := info.GetCode().GetJar()
	properties := map[string]string{}
	properties["shell.command.timeout.millisecs"] = "30000"
	flinkHome := "/zeppelin/flink/flink-1.12.3/"
	//flinkHome := "/Users/apple/develop/bigdata/flink-1.12.5"
	flinkUrl, _, err := jarExec.engineClient.GetEngineInfo(ctx, info.GetSpaceId(), info.GetArgs().GetClusterId())
	if err != nil {
		return nil, err
	}
	jarName, jarUrl, err := jarExec.resourceClient.GetFileById(ctx, jar.ResourceId)
	if err != nil {
		return nil, err
	}
	localJarPath := "/tmp/" + jarName
	udfs, err := jarExec.getUDFs(ctx, info.GetArgs().GetUdfs())
	if err != nil {
		return nil, err
	}
	jars := jarExec.getUDFJars(info.GetSpaceId(), udfs)
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("hdfs dfs -get %v %v\n", jarUrl, localJarPath))
	var udfJars []string
	for index, udf := range jars {
		var src = fmt.Sprintf("/tmp/%d.jar", index+1)
		udfJars = append(udfJars, src)
		builder.WriteString(fmt.Sprintf("hdfs dfs -get %v %v\n", udf, src))
	}
	builder.WriteString(fmt.Sprintf("%s/bin/flink run -d -m %s", flinkHome, flinkUrl))
	for _, udf := range udfJars {
		builder.WriteString(fmt.Sprintf(" -C %s", udf))
	}
	if info.GetArgs().GetParallelism() > 0 {
		builder.WriteString(fmt.Sprintf(" -p %d", info.GetArgs().GetParallelism()))
	}
	if jar.JarEntry != "" && len(jar.JarEntry) > 0 {
		builder.WriteString(fmt.Sprintf(" -c %s", jar.JarEntry))
	}
	builder.WriteString(fmt.Sprintf(" %s %s", localJarPath, jar.GetJarArgs()))
	code := builder.String()
	noteId, err = jarExec.initNote(ctx,"sh", info.GetInstanceId(), properties)
	if err != nil {
		return nil, err
	}

	result, err = jarExec.zeppelinClient.Submit("sh", "", noteId, code)
	if err != nil {
		return nil, err
	}
	if err = jarExec.preHandle(ctx, info.InstanceId, noteId, result.ParagraphId); err != nil {
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
							jarExec.logger.Error().Msg(fmt.Sprintf("flink cluster rest time out,cluster id is %s,job instanceId is %s",
								info.GetArgs().ClusterId, info.InstanceId))
							err = qerror.FlinkRestError
							return
						}
					}
				}
			}
			// TODO 如果没有异常，则结果写如数据库
			jarExec.postHandle(ctx, info.InstanceId, noteId, result)
		}
	}()
	for {
		if result, err = jarExec.zeppelinClient.QueryParagraphResult(noteId, result.ParagraphId); err != nil {
			return nil, err
		}
		jarExec.logger.Info().Msg(fmt.Sprintf("query result for instance %s , status %s, result %s", info.InstanceId, result.Status, result.Results))
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
	}
}

func (jarExec *JarExecutor) GetInfo(ctx context.Context, instanceId string, spaceId string, clusterId string) (*flink.Job, error) {
	return jarExec.getJobInfo(ctx, instanceId, spaceId, clusterId)
}

func (jarExec *JarExecutor) Cancel(ctx context.Context, instanceId string, spaceId string, clusterId string) error {
	return jarExec.cancelJob(ctx, instanceId, spaceId, clusterId)
}

func (jarExec *JarExecutor) Release(ctx context.Context, instanceId string) error {
	return jarExec.release(ctx, instanceId)
}

func (jarExec *JarExecutor) Validate(jobCode *model.StreamJobCode) (bool, string, error) {
	return true, "", nil
}
