package tests

import (
	"context"
	"fmt"
	"testing"

	"time"

	"github.com/DataWorkbench/glog"
	"github.com/stretchr/testify/require"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"

	"bytes"
	"crypto/rand"
	"math/big"

	"github.com/DataWorkbench/gproto/pkg/jobpb"
)

var infos []jobpb.RunJobRequest
var client jobpb.JobmanagerClient
var ctx context.Context

func CreateRandomString(len int) string {
	var container string
	var str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	b := bytes.NewBufferString(str)
	length := b.Len()
	bigInt := big.NewInt(int64(length))
	for i := 0; i < len; i++ {
		randomInt, _ := rand.Int(rand.Reader, bigInt)
		container += string(str[randomInt.Int64()])
	}
	return container
}

func mainInit(t *testing.T) {
	if len(infos) != 0 {
		return
	}

	// MySQL to pg

	infos = append(infos, jobpb.RunJobRequest{
		ID: CreateRandomString(20), WorkspaceID: "wsp-0123456789012345", NodeType: "ssql", Depends: `{"table":"sot-0123456789012345, sot-0123456789012346", "func": "","parallelism": "2","jobcpu": "2", "jobmem": "2048",  "taskcpu": "0.2", "taskmem": "256", "tasknum": "2" }`, MainRun: "insert into $qc$sot-0123456789012346$qc$ select * from $qc$sot-0123456789012345$qc$"})

	// kafka join common table
	infos = append(infos, jobpb.RunJobRequest{
		ID: CreateRandomString(20), WorkspaceID: "wsp-0123456789012345", NodeType: "ssql", Depends: `{"table":"sot-0123456789012349, sot-0123456789012350,sot-0123456789012351", "func": "","parallelism": "2","jobcpu": "2", "jobmem": "2048",  "taskcpu": "0.2", "taskmem": "256", "tasknum": "2" }`, MainRun: "insert into $qc$sot-0123456789012350$qc$ select  $qc$sot-0123456789012349$qc$.rate * $qc$sot-0123456789012351$qc$.paycount from  $qc$sot-0123456789012349$qc$, $qc$sot-0123456789012351$qc$ where $qc$sot-0123456789012351$qc$.paymoney =  $qc$sot-0123456789012349$qc$.dbmoney  "}) //{"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}

	// kafka join dimension table
	infos = append(infos, jobpb.RunJobRequest{
		ID: CreateRandomString(20), WorkspaceID: "wsp-0123456789012345", NodeType: "ssql", Depends: `{"table":"sot-0123456789012347, sot-0123456789012348,sot-0123456789012351", "func": "","parallelism": "2","jobcpu": "2", "jobmem": "2048",  "taskcpu": "0.2", "taskmem": "256", "tasknum": "2" }`, MainRun: " insert      into $qc$sot-0123456789012348$qc$ SELECT k.paycount * r.rate FROM $qc$sot-0123456789012351$qc$ AS k JOIN $qc$sot-0123456789012347$qc$ FOR SYSTEM_TIME AS OF k.tproctime AS r ON r.dbmoney = k.paymoney "})

	// jar
	infos = append(infos, jobpb.RunJobRequest{
		ID: CreateRandomString(20), WorkspaceID: "wsp-0123456789012345", NodeType: "jar", Depends: `{"func": "","parallelism": "2","jobcpu": "2", "jobmem": "2048",  "taskcpu": "0.2", "taskmem": "256", "tasknum": "2" , "jarargs":"--output  /tmp/output/` + time.Now().Format("2006-01-02_15-04-05") + `", "jarentry":"org.apache.flink.streaming.examples.wordcount.WordCount"}`, MainRun: "/home/lzzhang/bigdata/flink-bin-download/flink-job-artifacts/WordCount.jar"})

	address := "127.0.0.1:51001"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)

	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address:      address,
		LogLevel:     2,
		LogVerbosity: 99,
	})
	require.Nil(t, err, "%+v", err)
	client = jobpb.NewJobmanagerClient(conn)

	logger := glog.NewDefault()
	worker := idgenerator.New("")
	reqId, _ := worker.Take()

	ln := logger.Clone()
	ln.WithFields().AddString("rid", reqId)

	ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)
}

func TestJobManagerGRPC_RunJob(t *testing.T) {
	mainInit(t)

	for info := range infos {
		_, err := client.RunJob(ctx, &infos[info])
		require.Nil(t, err, "%+v", err)
	}
}

func TestJobManagerGRPC_GetJobStatus(t *testing.T) {
	var req jobpb.GetJobStatusRequest

	mainInit(t)
	req.ID = infos[0].ID

	rep, err := client.GetJobStatus(ctx, &req)
	require.Nil(t, err, "%+v", err)
	fmt.Println(rep)
}

func TestJobManagerGRPC_CancelJob(t *testing.T) {
	var req jobpb.CancelJobRequest

	mainInit(t)
	req.ID = infos[0].ID
	req.ID = "JT6o56gIFoYhMKhBktAK"

	_, err := client.CancelJob(ctx, &req)
	require.Nil(t, err, "%+v", err)
}
