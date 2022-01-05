package tests

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/flinkpb"
	"github.com/DataWorkbench/gproto/pkg/jobpb"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	client     jobpb.JobmanagerClient
	ctx        context.Context
	spaceId    = "wks-0123456789012345"
	instanceId = "syx-JHGYFjhKwUfaQDHZ"
	clusterId  = "cfi-05636e792cfe5000"
)

func init() {
	address := "127.0.0.1:9105"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)

	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{Address: address})
	if err != nil {
		panic(err)
	}
	client = jobpb.NewJobmanagerClient(conn)
}

func Test_RunSql(t *testing.T) {
	args := model.StreamJobArgs{
		ClusterId:         "cfi-05636e792cfe5000",
		Parallelism:       0,
		Udfs:              nil,
		Connectors:        nil,
		BuiltInConnectors: nil,
	}
	jobType := model.StreamJob_SQL

	sql := flinkpb.FlinkSQL{Code: "" +
		"create table if not exists datagen(id int,name string) with ('connector' = 'datagen','rows-per-second' = '2');" +
		"create table if not exists print(id int,name string) with ('connector' = 'print');" +
		"create table if not exists print2(id int) with ('connector' = 'print');" +
		"insert into print select * from datagen;" +
		"insert into print2 select id from datagen;"}
	code := model.StreamJobCode{
		Type:      jobType,
		Operators: nil,
		Sql:       &sql,
		Jar:       nil,
		Scala:     nil,
		Python:    nil,
	}
	req := request.RunJob{
		InstanceId:    "syx-JHGYFjhKwUfaQDHZ",
		SpaceId:       spaceId,
		Args:          &args,
		Code:          &code,
		SavepointPath: "",
	}
	job, err := client.RunJob(ctx, &req)
	require.Nil(t, err)
	fmt.Println(job)
}

func Test_Validate(t *testing.T) {
	var code = "create table if not exists datagen(id int,name string);" +
		"create table if not exists print(id int,name string);" +
		"insert into print select * from datagen;"

	sql := flinkpb.FlinkSQL{Code: code}
	jobCode := model.StreamJobCode{
		Type:      model.StreamJob_SQL,
		Operators: nil,
		Sql:       &sql,
		Jar:       nil,
		Scala:     nil,
		Python:    nil,
	}
	req := request.ValidateJob{Code: &jobCode}
	res, err := client.ValidateJob(ctx, &req)
	require.Nil(t, err)
	fmt.Println(res.Message)
}

func Test_GetInfo(t *testing.T) {
	req := request.GetJobInfo{
		InstanceId: instanceId,
		SpaceId:    spaceId,
		ClusterId:  clusterId,
		Type:       model.StreamJob_SQL,
	}
	info, err := client.GetJobInfo(ctx, &req)
	require.Nil(t, err)
	fmt.Println(info)
}

func Test_Cancel(t *testing.T) {
	req := request.CancelJob{
		InstanceId: instanceId,
		SpaceId:    spaceId,
		ClusterId:  clusterId,
		Type:       model.StreamJob_SQL,
	}
	_, err := client.CancelJob(ctx, &req)
	require.Nil(t, err)
}
