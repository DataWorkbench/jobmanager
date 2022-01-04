package flink

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/gproto/pkg/model"
	"strings"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type JarExecutor struct {
	bm  *BaseExecutor
	ctx context.Context
}

func NewJarExecutor(bm *BaseExecutor, ctx context.Context) *JarExecutor {
	return &JarExecutor{
		bm:  bm,
		ctx: ctx,
	}
}

func (jarExec *JarExecutor) Run(ctx context.Context, info *request.RunJob) (*zeppelin.ExecuteResult, error) {
	jar := info.GetCode().GetJar()
	properties := map[string]string{}
	properties["shell.command.timeout.millisecs"] = "30000"
	//flinkHome := "/zeppelin/flink/flink-1.12.3/"
	flinkHome := "/Users/apple/develop/bigdata/flink-1.12.5"
	flinkUrl, _, err := jarExec.bm.engineClient.GetEngineInfo(ctx, info.GetSpaceId(), info.GetArgs().GetClusterId())
	if err != nil {
		return nil, err
	}
	jarName, jarUrl, err := jarExec.bm.resourceClient.GetFileById(ctx, jar.ResourceId)
	if err != nil {
		return nil, err
	}
	localJarPath := "/tmp/" + jarName
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("hdfs dfs -get %v %v\n", jarUrl, localJarPath))
	builder.WriteString(fmt.Sprintf("%s/bin/flink run -d -m %s", flinkHome, flinkUrl))
	if info.GetArgs().GetParallelism() > 0 {
		builder.WriteString(fmt.Sprintf(" -p %d", info.GetArgs().GetParallelism()))
	}
	if jar.JarEntry != "" && len(jar.JarEntry) > 0 {
		builder.WriteString(fmt.Sprintf(" -c %s", jar.JarEntry))
	}
	builder.WriteString(fmt.Sprintf(" %s %s", localJarPath, jar.GetJarArgs()))
	code := builder.String()

	session := zeppelin.NewZSessionWithProperties(jarExec.bm.zeppelinConfig, SHELL, properties)
	if err = session.Start(); err != nil {
		return nil, err
	}

	result, err := session.Exec(code)
	if err != nil {
		return nil, err
	}

	defer func() {
		if result != nil && (len(result.Results) > 0 || len(result.JobId) == 32) {
			if len(result.JobId) != 32 && (result.Status.IsRunning() || result.Status.IsPending()) {
				_ = session.Stop()
			} else {
				jobInfo := jarExec.bm.TransResult(info.SpaceId, info.InstanceId, result)
				if err := jarExec.bm.UpsertResult(ctx, jobInfo); err != nil {
					_ = session.Stop()
				}
			}
		}
	}()
	if result.Results != nil && len(result.Results) > 0 {
		for _, re := range result.Results {
			if strings.EqualFold(re.Type, "TEXT") {
				jobInfo := strings.Split(re.Data, "JobID ")
				if len(jobInfo) == 2 {
					if jobId := strings.ReplaceAll(jobInfo[1], "\n", ""); len(jobId) == 32 {
						result.JobId = jobId
						return result, nil
					}
				}
			}
		}
	}
	return result, nil
}

func (jarExec *JarExecutor) GetInfo(ctx context.Context, instanceId string, spaceId string, clusterId string) (*flink.Job, error) {
	return jarExec.bm.GetJobInfo(ctx, instanceId, spaceId, clusterId)
}

func (jarExec *JarExecutor) Cancel(ctx context.Context, instanceId string, spaceId string, clusterId string) error {
	return jarExec.bm.CancelJob(ctx, instanceId, spaceId, clusterId)
}

func (jarExec *JarExecutor) Validate(jobCode *model.StreamJobCode) (bool, string, error) {
	return true, "", nil
}
