package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DataWorkbench/glog"
	"github.com/stretchr/testify/require"

	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/utils/idgenerator"

	"bytes"
	"crypto/rand"
	"math/big"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/DataWorkbench/gproto/pkg/jobpb"
)

var mspd jobpb.RunJobRequest           //regress test
var mspdcancel jobpb.RunJobRequest     //regress test
var mspdcancelall1 jobpb.RunJobRequest //regress test
var mspdcancelall2 jobpb.RunJobRequest //regress test
var mspdpg jobpb.RunJobRequest         //manual test
var mw jobpb.RunJobRequest             //manual test
var mc jobpb.RunJobRequest             //manual test
var s3 jobpb.RunJobRequest             //manual test
var jar jobpb.RunJobRequest            //manual test
var ck jobpb.RunJobRequest             //manual test
var udfScala jobpb.RunJobRequest       //manual test
var udfJar jobpb.RunJobRequest         //manual test

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

func typeToJsonString(v interface{}) string {
	s, _ := json.Marshal(&v)
	return string(s)
}

var client jobpb.JobmanagerClient
var ctx context.Context
var spaceID45 string

func mainInit(t *testing.T, manualInit bool) {
	// create table in mysql
	dsn := "root:123456@tcp(127.0.0.1:3306)/data_workbench"
	db, cerr := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	require.Nil(t, cerr, "%+v", cerr)
	db.Exec("CREATE TABLE IF NOT EXISTS ms(id bigint, id1 bigint)")
	db.Exec("CREATE TABLE IF NOT EXISTS pd(id bigint, id1 bigint)")
	db.Exec("insert into ms values(1, 1)")
	db.Exec("insert into ms values(2, 2)")
	if manualInit == true {
		db.Exec("CREATE TABLE IF NOT EXISTS mc(rate bigint, dbmoney varchar(10))")
		db.Exec("CREATE TABLE IF NOT EXISTS mcd(total bigint)")
		db.Exec("CREATE TABLE IF NOT EXISTS mw(rate bigint, dbmoney varchar(10))")
		db.Exec("CREATE TABLE IF NOT EXISTS mwd(total bigint)")
		db.Exec("insert into mc values(2, 'EUR')")
		db.Exec("insert into mc values(7, 'USD')")
		db.Exec("insert into mw values(2, 'EUR')")
		db.Exec("insert into mw values(7, 'USD')")
		db.Exec("CREATE TABLE IF NOT EXISTS udfs(a varchar(10))")
		db.Exec("CREATE TABLE IF NOT EXISTS udfd(a varchar(10))")
		db.Exec("insert into udfs values('abcdef')")
	}
	spaceID45 = "wks-0123456789012345"

	mspd = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012347", "sot-0123456789012348"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012348$qc$ select * from $qc$sot-0123456789012347$qc$"})}
	mspdcancel = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012347", "sot-0123456789012348"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012348$qc$ select * from $qc$sot-0123456789012347$qc$"})}
	mspdcancelall1 = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012347", "sot-0123456789012348"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012348$qc$ select * from $qc$sot-0123456789012347$qc$"})}
	mspdcancelall2 = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012347", "sot-0123456789012348"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012348$qc$ select * from $qc$sot-0123456789012347$qc$"})}
	mspdpg = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012347", "sot-0123456789012345"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012345$qc$ select * from $qc$sot-0123456789012347$qc$"})}
	mc = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012357", "sot-0123456789012358", "sot-0123456789012346"}, MainRun: "insert into $qc$sot-0123456789012358$qc$ select  $qc$sot-0123456789012357$qc$.rate * $qc$sot-0123456789012346$qc$.paycount from  $qc$sot-0123456789012357$qc$, $qc$sot-0123456789012346$qc$ where $qc$sot-0123456789012346$qc$.paymoney =  $qc$sot-0123456789012357$qc$.dbmoney  "})}
	//{"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}
	mw = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012355", "sot-0123456789012356", "sot-0123456789012346"}, Parallelism: 2, JobCpu: 2, JobMem: 2, TaskCpu: 0.2, TaskMem: 256, TaskNum: 2, MainRun: "insert into $qc$sot-0123456789012356$qc$ SELECT k.paycount * r.rate FROM $qc$sot-0123456789012346$qc$ AS k JOIN $qc$sot-0123456789012355$qc$ FOR SYSTEM_TIME AS OF k.tproctime AS r ON r.dbmoney = k.paymoney "})}
	jar = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkJob, Depends: typeToJsonString(constants.FlinkJob{Parallelism: 2, JobCpu: 2, JobMem: 2, TaskCpu: 0.2, TaskMem: 2, TaskNum: 2, JarArgs: "", JarEntry: "org.apache.flink.streaming.examples.wordcount.WordCount", MainRun: "/home/lzzhang/bigdata/flink-bin-download/flink-job-artifacts/WordCount.jar"})}
	s3 = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012359", "sot-0123456789012360"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012360$qc$ select * from $qc$sot-0123456789012359$qc$"})}
	ck = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012361"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012361$qc$ values(6, 6)"})}
	udfScala = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012362", "sot-0123456789012363"}, Funcs: []string{"udf-0123456789012345"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012363$qc$ select $qc$udf-0123456789012345$qc$(a) from $qc$sot-0123456789012362$qc$"})}
	udfJar = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012362", "sot-0123456789012363"}, Funcs: []string{"udf-0123456789012351"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012363$qc$ select $qc$udf-0123456789012351$qc$(a) from $qc$sot-0123456789012362$qc$"})}

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

func Test_RunJob(t *testing.T) {
	mainInit(t, false)

	_, err := client.RunJob(ctx, &mspd)
	require.Nil(t, err, "%+v", err)
}

func Test_GetJobStatus(t *testing.T) {
	for {
		var req jobpb.GetJobStatusRequest
		req.ID = mspd.ID

		rep, err := client.GetJobStatus(ctx, &req)
		require.Nil(t, err, "%+v", err)
		if rep.Status == constants.InstanceStateRunning {
			time.Sleep(time.Second)
		} else if rep.Status == constants.InstanceStateFailed {
			require.Equal(t, "success", "failed")
		} else if rep.Status == constants.InstanceStateSucceed {
			break
		}
	}
}

func Test_CancelJob(t *testing.T) {
	var req jobpb.CancelJobRequest
	var err error

	_, err = client.RunJob(ctx, &mspdcancel)
	require.Nil(t, err, "%+v", err)

	req.ID = mspdcancel.ID

	_, err = client.CancelJob(ctx, &req)
	require.Nil(t, err, "%+v", err)
}

func Test_CancelAllJob(t *testing.T) {
	var req jobpb.CancelAllJobRequest
	var err error

	req.SpaceID = spaceID45
	time.Sleep(time.Second * 10)

	go client.RunJob(ctx, &mspdcancelall1)
	go client.RunJob(ctx, &mspdcancelall2)

	time.Sleep(time.Second * 10)
	_, err = client.CancelAllJob(ctx, &req)
	require.Nil(t, err, "%+v", err)
}

func Test_RunJobManual(t *testing.T) {
	//mainInit(t, true)
	//var err error

	//_, err = client.RunJob(ctx, &ck)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.RunJob(ctx, &s3)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.RunJob(ctx, &mspdpg)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.RunJob(ctx, &mc)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.RunJob(ctx, &mw)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.RunJob(ctx, &jar)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.RunJob(ctx, &udfScala)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.RunJob(ctx, &udfJar)
	//require.Nil(t, err, "%+v", err)
}
