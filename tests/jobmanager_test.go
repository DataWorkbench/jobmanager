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
var spaceId string = "wks-0123456789012345"

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

func Test_Run(t *testing.T) {
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
