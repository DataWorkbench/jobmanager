package executor

import (
	"context"
	"strings"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/jobmanager/utils"
)

var (
	Quote    = "$qc$"
	UDFQuote = Quote + "_udf_name_" + Quote
)

type BaseManagerExecutor struct {
	engineClient   utils.EngineClient
	udfClient      utils.UdfClient
	resourceClient utils.ResourceClient
	flinkClient    *flink.Client
}

func NewBaseManager(engineClient utils.EngineClient, udfClient utils.UdfClient, resourceClient utils.ResourceClient, flinkConfig flink.ClientConfig) *BaseManagerExecutor {
	return &BaseManagerExecutor{
		engineClient:   engineClient,
		udfClient:      udfClient,
		resourceClient: resourceClient,
		flinkClient:    flink.NewFlinkClient(flinkConfig),
	}
}

func (bm *BaseManagerExecutor) getEngineInfo(ctx context.Context, spaceId string, clusterId string) (url string, version string, err error) {
	api, err := bm.engineClient.Client.DescribeFlinkClusterAPI(ctx, &request.DescribeFlinkClusterAPI{
		SpaceId:   spaceId,
		ClusterId: clusterId,
	})
	if err != nil {
		return
	}
	url = api.URL
	version = api.Version
	return
}

func (bm *BaseManagerExecutor) getConnectors(builtInConnectors []string, flinkVersion string) string {
	var executeJars string
	var connectorSet map[string]string
	connectorJarMap := constants.FlinkConnectorJarMap[flinkVersion]
	for _, connector := range builtInConnectors {
		jars := connectorJarMap[connector]
		for _, jar := range jars {
			connectorSet[jar+","] = ""
		}
	}
	for jar, _ := range connectorSet {
		executeJars += jar
	}
	if executeJars != "" && len(executeJars) > 0 && strings.HasSuffix(executeJars, ",") {
		executeJars = executeJars[:strings.LastIndex(executeJars, ",")-1]
	}
	return executeJars
}

type Udf struct {
	udfType model.UDFInfo_Language
	code    string
}

func (bm *BaseManagerExecutor) getUDFs(ctx context.Context, udfIds []string) ([]*Udf, error) {
	var udfCodes []*Udf
	for _, udfId := range udfIds {
		var (
			udfLanguage model.UDFInfo_Language
			udfDefine   string
			udfName     string
		)
		udfLanguage, udfName, udfDefine, err := bm.udfClient.DescribeUdfManager(ctx, udfId)
		if err != nil {
			return nil, err
		}

		if udfLanguage != model.UDFInfo_Java {
			udfDefine = strings.Replace(udfDefine, UDFQuote, udfName, -1)
		}
		udf := Udf{
			udfType: udfLanguage,
			code:    udfDefine,
		}
		udfCodes = append(udfCodes, &udf)
	}
	return udfCodes, nil
}

func (bm *BaseManagerExecutor) getUDFJars(udfs []*Udf) string {
	var executionUdfJars string
	for _, udf := range udfs {
		if udf.udfType == model.UDFInfo_Java {
			executionUdfJars += udf.code + ","
		}
	}
	if strings.HasSuffix(executionUdfJars, ",") {
		executionUdfJars = executionUdfJars[:len(executionUdfJars)-1]
	}
	return executionUdfJars
}

func (bm *BaseManagerExecutor) getGlobalProperties(ctx context.Context, info *request.JobInfo, udfs []*Udf) (map[string]string, error) {
	spaceId := info.GetSpaceId()
	clusterId := info.GetArgs().GetClusterId()
	properties := map[string]string{}
	flinkUrl, flinkVersion, err := bm.getEngineInfo(ctx, spaceId, clusterId)
	if err != nil {
		return nil, err
	}
	host := flinkUrl[:strings.Index(flinkUrl, ":")]
	port := flinkUrl[strings.Index(flinkUrl, ":")+1:]
	if host != "" && len(host) > 0 && port != "" && len(port) > 0 {
		properties["flink.execution.remote.host"] = host
		properties["flink.execution.remote.port"] = port
	} else {
		return nil, qerror.ParseEngineFlinkUrlFailed.Format(flinkUrl)
	}

	executionJars := bm.getConnectors(info.GetArgs().GetBuiltInConnectors(), flinkVersion)
	if executionJars != "" && len(executionJars) > 0 {
		properties["flink.execution.jars"] = executionJars
	}
	udfJars := bm.getUDFJars(udfs)
	if udfJars != "" && len(udfJars) > 0 {
		properties["flink.udf.jars"] = udfJars
	}
	return properties, nil
}

func (bm *BaseManagerExecutor) GetJobInfo(ctx context.Context, jobId string, jobName string, spaceId string, clusterId string) (*flink.Job, error) {
	engineRes, err := bm.engineClient.Client.DescribeFlinkClusterAPI(ctx, &request.DescribeFlinkClusterAPI{
		SpaceId:   spaceId,
		ClusterId: clusterId,
	})
	if err != nil {
		return nil, err
	}
	flinkUrl := engineRes.GetURL()
	return bm.flinkClient.GetJobInfo(flinkUrl, jobId, jobName)
}

func (bm *BaseManagerExecutor) CancelJob(ctx context.Context, jobId string, spaceId string, clusterId string) error {
	engineRes, err := bm.engineClient.Client.DescribeFlinkClusterAPI(ctx, &request.DescribeFlinkClusterAPI{
		SpaceId:   spaceId,
		ClusterId: clusterId,
	})
	if err != nil {
		return err
	}
	flinkUrl := engineRes.GetURL()
	return bm.flinkClient.CancelJob(flinkUrl, jobId)
}
