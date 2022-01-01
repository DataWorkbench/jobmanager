package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/request"
)

type JarManagerExecutor struct {
	bm             *BaseManagerExecutor
	zeppelinConfig zeppelin.ClientConfig
	ctx            context.Context
	logger         *glog.Logger
}

func NewJarManagerExecutor(bm *BaseManagerExecutor, zeppelinConfig zeppelin.ClientConfig, ctx context.Context, logger *glog.Logger) *JarManagerExecutor {
	return &JarManagerExecutor{
		bm:             bm,
		zeppelinConfig: zeppelinConfig,
		ctx:            ctx,
		logger:         logger,
	}
}

func (jarExec *JarManagerExecutor) Run(ctx context.Context, info *request.JobInfo) (*zeppelin.ExecuteResult, error) {
	jar := info.GetCode().GetJar()
	properties := map[string]string{}
	properties["shell.command.timeout.millisecs"] = "30000"
	properties["shell.working.directory.user.home"] = "/zeppelin/flink/flink-1.12.3/"
	flinkUrl, _, err := jarExec.bm.getEngineInfo(ctx, info.GetSpaceId(), info.GetArgs().GetClusterId())
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
	builder.WriteString(fmt.Sprintf("/bin/flink run -d -m %s", flinkUrl))
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
	result, err := session.Exec(code)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (jarExec *JarManagerExecutor) GetInfo(ctx context.Context, jobId string, jobName string, spaceId string, clusterId string) (*flink.Job, error) {
	return jarExec.bm.GetJobInfo(ctx, jobId, jobName, spaceId, clusterId)
}

func (jarExec *JarManagerExecutor) Cancel(ctx context.Context, jobId string, spaceId string, clusterId string) error {
	return jarExec.bm.CancelJob(ctx, jobId, spaceId, clusterId)
}