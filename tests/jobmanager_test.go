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

var client jobpb.JobmanagerClient
var ctx context.Context
var spaceId = "wks-0123456789012345"

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
		"insert into print select * from datagen;"}
	code := model.StreamJobCode{
		Type:      jobType,
		Operators: nil,
		Sql:       &sql,
		Jar:       nil,
		Scala:     nil,
		Python:    nil,
	}
	req := request.JobInfo{
		JobId:         "syx-JHGYFjhKwUfaQDHZ",
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
		"create table if not exists print(,id int,name string);" +
		"insert into print select * from datagen;"
	req := request.JobValidate{
		Type: model.StreamJob_SQL,
		Code: code,
	}
	res, err := client.Validate(ctx, &req)
	require.Nil(t, err)
	fmt.Println(res.Flag, res.Message)
}

func Test_GetInfo(t *testing.T) {
	var flinkId = "8aa2bb960972ce7351745321bfc8dde7"
	req := request.JobMessage{
		JobId:     "syx-JHGYFjhKwUfaQDHZ",
		FlinkId:   flinkId,
		SpaceId:   spaceId,
		ClusterId: "cfi-05636e792cfe5000",
		Type:      model.StreamJob_SQL,
	}
	info, err := client.GetJobInfo(ctx, &req)
	require.Nil(t, err)
	fmt.Println(info)
}
