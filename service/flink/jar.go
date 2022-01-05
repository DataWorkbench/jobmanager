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
	*BaseExecutor
	ctx context.Context
}

func NewJarExecutor(bm *BaseExecutor, ctx context.Context) *JarExecutor {
	return &JarExecutor{
		BaseExecutor: bm,
		ctx:          ctx,
	}
}

func (jarExec *JarExecutor) Run(ctx context.Context, info *request.RunJob) (*zeppelin.ExecuteResult, error) {
	//if result, err := jarExec.PreConn(ctx, info.InstanceId); err != nil {
	//	return nil, err
	//} else if result != nil {
	//	return result, nil
	//}

	jar := info.GetCode().GetJar()
	properties := map[string]string{}
	properties["shell.command.timeout.millisecs"] = "30000"
	//flinkHome := "/zeppelin/flink/flink-1.12.3/"
	flinkHome := "/Users/apple/develop/bigdata/flink-1.12.5"
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
	jars := jarExec.getUDFJars(udfs)

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

	session := zeppelin.NewZSessionWithProperties(jarExec.zeppelinConfig, SHELL, properties)
	if err = session.Start(); err != nil {
		return nil, err
	}

	result, err := session.Sub(code)
	if err != nil {
		return nil, err
	}
	if err = jarExec.PreHandle(ctx, info.SpaceId, info.InstanceId, result); err != nil {
		return nil, err
	}
	if result, err = session.WaitUntilFinished(result.StatementId); err != nil {
		return nil, err
	}

	defer func() {
		jarExec.PostHandle(ctx, info.SpaceId, info.InstanceId, result, session)
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
	return jarExec.GetJobInfo(ctx, instanceId, spaceId, clusterId)
}

func (jarExec *JarExecutor) Cancel(ctx context.Context, instanceId string, spaceId string, clusterId string) error {
	return jarExec.CancelJob(ctx, instanceId, spaceId, clusterId)
}

func (jarExec *JarExecutor) Validate(jobCode *model.StreamJobCode) (bool, string, error) {
	return true, "", nil
}
