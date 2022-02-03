package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcdeveloper"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel"
	"github.com/DataWorkbench/gproto/xgo/types/pbmodel/pbflink"
	"github.com/DataWorkbench/gproto/xgo/types/pbrequest"
	"github.com/stretchr/testify/require"
)

var (
	client     pbsvcdeveloper.JobManageClient
	ctx        context.Context
	spaceId    = "wks-0123456789012345"
	instanceId = "syx-JHGYFjhKwUfaQDHZ"
	//instanceId2 = "syx-JHGYFjhKwUfaQDHQ"
	clusterId = "cfi-05636e792cfe5000"
)

func init() {
	address := "127.0.0.1:9105"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)

	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{Address: address})
	if err != nil {
		panic(err)
	}
	client = pbsvcdeveloper.NewJobManageClient(conn)
}

func Test_JobID(t *testing.T) {
	var flinkId = "f7083d6497d90bb8e408c9419d8d858e"
	fmt.Println(len(flinkId))
}

func Test_InitSql(t *testing.T) {
	args := pbmodel.StreamJobArgs{
		ClusterId:         clusterId,
		Udfs:              nil,
		Connectors:        nil,
		BuiltInConnectors: nil,
	}
	jobType := pbmodel.StreamJob_SQL

	sql := pbflink.FlinkSQL{Code: "" +
		"create table if not exists datagen(id int,name string) with ('connector' = 'datagen','rows-per-second' = '2');\n" +
		"create table if not exists print(id int,name string) with ('connector' = 'print');\n" +
		"create table if not exists print2(id int) with ('connector' = 'print');\n" +
		"-- insert into print select * from datagen;\n" +
		"-- insert into print2 select id from datagen;\n" +
		"select * from datagen;"}
	code := pbmodel.StreamJobCode{
		Type:      jobType,
		Operators: nil,
		Sql:       &sql,
		Jar:       nil,
		Scala:     nil,
		Python:    nil,
	}
	req := pbrequest.InitFlinkJob{
		InstanceId:    instanceId,
		SpaceId:       spaceId,
		Args:          &args,
		Code:          &code,
		SavepointPath: "",
	}
	job, err := client.InitFlinkJob(ctx, &req)
	require.Nil(t, err)
	fmt.Println(job)
}

func Test_InitJar(t *testing.T) {

}

func Test_Run(t *testing.T) {
	req := pbrequest.SubmitFlinkJob{
		InstanceId:  instanceId,
		NoteId:      "2GUZUBUCJ",
		ParagraphId: "paragraph_1642342087611_47669399",
		Type:        pbmodel.StreamJob_SQL,
	}
	job, err := client.SubmitFlinkJob(ctx, &req)
	require.Nil(t, err)
	fmt.Println(job)
}

func Test_GetInfo(t *testing.T) {
	req := pbrequest.GetFlinkJob{
		FlinkId:   "6a96262d85938151cecc75c20da039ef",
		SpaceId:   spaceId,
		ClusterId: clusterId,
	}
	job, err := client.GetFlinkJob(ctx, &req)
	if err != nil {
		t.Error(err)
	} else {
		fmt.Println(job)
	}
}
