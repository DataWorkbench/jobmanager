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
var udf jobpb.RunJobRequest            //manual test

//TODO
//var pg jobpb.RunJobRequest       //manual test
//var s3 jobpb.RunJobRequest           //manual test
//var ck jobpb.RunJobRequest           //manual test
//var hdfs jobpb.RunJobRequest           //manual test
//var ftp jobpb.RunJobRequest           //manual test
var mw jobpb.RunJobRequest           //manual test
var SqlJobAndUdf jobpb.RunJobRequest //manual test
var Hbase jobpb.RunJobRequest        //manual test
var PythonTable jobpb.RunJobRequest  //manual test
var PythonPrint jobpb.RunJobRequest  //manual test
var ScalaPrint jobpb.RunJobRequest   //manual test

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
	dsn := "root:password@tcp(127.0.0.1:3306)/data_workbench"
	db, cerr := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	require.Nil(t, cerr, "%+v", cerr)
	db.Exec("CREATE TABLE IF NOT EXISTS ms(id bigint, id1 bigint)")
	db.Exec("CREATE TABLE IF NOT EXISTS pd(id bigint, id1 bigint)")
	db.Exec("delete from ms")
	db.Exec("delete from pd")
	db.Exec("insert into ms values(1, 1)")
	db.Exec("insert into ms values(2, 2)")
	db.Exec("insert into workspace values('wks-0123456789012345', 'usr-ndcbIwIa', 'lzz', 'lzz', 1, now(), now());")
	if manualInit == true {
		db.Exec("CREATE TABLE IF NOT EXISTS mw(rate bigint, dbmoney varchar(10))")
		db.Exec("CREATE TABLE IF NOT EXISTS mwd(total bigint)")
		db.Exec("insert into mw values(2, 'EUR')")
		db.Exec("insert into mw values(7, 'USD')")
	}
	spaceID45 = "wks-0123456789012345"

	mspd = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"id1","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
	mw = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012346","table":"billing as k","distinct":"ALL","column":[{"field":"k.paycount * r.rate","as":"stotal"}]}},{"nodetype":"Source","nodeid":"rxx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012355","table":"mw FOR SYSTEM_TIME AS OF k.tproctime AS r","distinct":"ALL","column":null}},{"nodetype":"Join","nodeid":"xx1","upstream":"xx0","upstreamright":"rxx0","downstream":"xx2","pointx":"","pointy":"","property":{"join":"JOIN","expression":"r.dbmoney = k.paymoney","column":[{"field":"stotal","as":""}]}},{"nodetype":"Dest","nodeid":"xx2","upstream":"xx1","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"mwd","column":["total"],"id":"sot-0123456789012356"}}]}`}
	//{"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}
	mspdcancel = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"id1","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
	mspdcancelall1 = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"id1","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
	mspdcancelall2 = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"id1","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
	udf = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"javatwice(id1)","as":"id1"}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
	jar = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":false,"env":{"engine_id":"","parallelism":0,"jobcu":2,"taskcu":2,"tasknum":2,"custom":null},"nodes":[{"nodetype":"Jar","nodeid":"xxxx","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"jar_args":"","jar_entry":"spendreport.FraudDetectionJob","jar_id":"fil-04bbca8755d62131","accesskey":"","secretkey":"","endpoint":"","hbasehosts":""}}]}`}
	ftp = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012366","table":"ftpsource","distinct":"ALL","column":[{"field":"readName","as":"readName"},{"field":"cellPhone","as":"cellPhone"},{"field":"universityName","as":"universityName"},{"field":"city","as":"city"},{"field":"street","as":"street"},{"field":"ip","as":"ip"}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"ftpdest","column":["readName","cellPhone","universityName","city","street","ip"],"id":"sot-0123456789012367"}}]}`}

	//s3 = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012359", "sot-0123456789012360"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012360$qc$ select * from $qc$sot-0123456789012359$qc$"})}
	//ck = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012361"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012361$qc$ values(6, 6)"})}
	SqlJobAndUdf = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Sql","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"sql":"\n\ndrop table if exists pd;\ncreate table pd\n(id bigint , id1 bigint COMMENT 'xxx' , PRIMARY KEY (id) NOT ENFORCED) WITH (\n'connector' = 'jdbc',\n'url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench',\n'table-name' = 'pd',\n'username' = 'root',\n'password' = 'password'\n);\n\n\ndrop table if exists ms;\ncreate table ms\n(id bigint , id1 bigint COMMENT 'xxx' , PRIMARY KEY (id) NOT ENFORCED) WITH (\n'connector' = 'jdbc',\n'url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench',\n'table-name' = 'ms',\n'username' = 'root',\n'password' = 'password'\n);\n\n\n\n\ninsert into pd(id,id1)  select ALL plus_one(id) as id, ascii(python_upper('a')) from ms "}}]}`}
	//insert into pd(id,id1)  select ALL id as id,id1 from ms
	// = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: }
	Hbase = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012364","table":"testsource","distinct":"ALL","column":[{"field":"rowkey","as":""},{"field":"columna","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"testdest","column":["rowkey","columna"],"id":"sot-0123456789012365"}}]}`}
	PythonTable = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Python","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"code":"from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic \nfrom pyflink.table import StreamTableEnvironment, EnvironmentSettings \ndef insertselect():\n    t_env = StreamTableEnvironment.create(stream_execution_environment=s_env)\n    source_ddl = \"create table ms(id bigint , id1 bigint COMMENT 'xxx' , PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench','table-name' = 'ms','username' = 'root','password' = 'password')\" \n    sink_ddl =   \"create table pd(id bigint , id1 bigint COMMENT 'xxx' , PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench','table-name' = 'pd','username' = 'root','password' = 'password')\" \n    t_env.execute_sql(source_ddl) \n    t_env.execute_sql(sink_ddl) \n    t_env.sql_query(\"SELECT * FROM ms\").insert_into(\"pd\") \n    t_env.execute(\"demo\") \nif __name__ == '__main__':\n    insertselect()"}}]}`}
	PythonPrint = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Python","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"code":"print('hello world')"}}]}`}
	ScalaPrint = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Scala","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"code":"println(\"hello world\")"}}]}`}

	address := "127.0.0.1:9105"
	lp := glog.NewDefault()
	ctx = glog.WithContext(context.Background(), lp)

	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
		Address: address,
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

func Test_Run(t *testing.T) {
	mainInit(t, false)

	_, err := client.Run(ctx, &mspd)
	require.Nil(t, err, "%+v", err)
}

func Test_Syntax(t *testing.T) {
	mainInit(t, false)

	_, err := client.Syntax(ctx, &mspd)
	require.Nil(t, err, "%+v", err)
}
func Test_Preview(t *testing.T) {
	mainInit(t, false)

	_, err := client.Preview(ctx, &mspd)
	require.Nil(t, err, "%+v", err)
}
func Test_Explain(t *testing.T) {
	mainInit(t, false)

	_, err := client.Explain(ctx, &mspd)
	require.Nil(t, err, "%+v", err)
}

func Test_GetJobStatus(t *testing.T) {
	mainInit(t, false)

	_, err := client.Run(ctx, &mspd)
	require.Nil(t, err, "%+v", err)

	for {
		var req jobpb.GetJobStateRequest
		req.ID = mspd.ID

		rep, err := client.GetJobState(ctx, &req)
		require.Nil(t, err, "%+v", err)
		if rep.State == constants.InstanceStateRunning {
			time.Sleep(time.Second)
		} else if rep.State == constants.InstanceStateFailed {
			require.Equal(t, "success", "failed")
		} else if rep.State == constants.InstanceStateSucceed {
			break
		}
	}
}

func Test_CancelJob(t *testing.T) {
	var req jobpb.CancelJobRequest
	var err error

	mainInit(t, false)

	_, err = client.Run(ctx, &mspdcancel)
	require.Nil(t, err, "%+v", err)

	req.ID = mspdcancel.ID

	_, err = client.CancelJob(ctx, &req)
	require.Nil(t, err, "%+v", err)
}

func Test_CancelAllJob(t *testing.T) {
	//	var req jobpb.CancelAllJobRequest
	//	var err error
	// TODO
	//
	//	mainInit(t, false)
	//
	//	req.SpaceID = spaceID45
	//	time.Sleep(time.Second * 10)
	//
	//	go client.Run(ctx, &mspdcancelall1)
	//	go client.Run(ctx, &mspdcancelall2)
	//
	//	time.Sleep(time.Second * 10)
	//	_, err = client.CancelAllJob(ctx, &req)
	//	require.Nil(t, err, "%+v", err)
}

func Test_RunUdfSql(t *testing.T) {
	mainInit(t, false)
	var err error
	_, err = client.Run(ctx, &udf)
	require.Nil(t, err, "%+v", err)
}

func Test_RunJar(t *testing.T) {
	mainInit(t, false)
	var err error
	_, err = client.Run(ctx, &jar)
	require.Nil(t, err, "%+v", err)
}

func Test_RunFtp(t *testing.T) {
	mainInit(t, false)
	var err error
	_, err = client.Run(ctx, &ftp)
	require.Nil(t, err, "%+v", err)
}

func Test_RunManual(t *testing.T) {
	mainInit(t, true)
	var err error

	_, err = client.Run(ctx, &mw)
	require.Nil(t, err, "%+v", err)

	//_, err = client.Run(ctx, &SqlJobAndUdf)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.Run(ctx, &Hbase)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.Run(ctx, &PythonTable)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.Run(ctx, &PythonPrint)
	//require.Nil(t, err, "%+v", err)

	//_, err = client.Run(ctx, &ScalaPrint)
	//require.Nil(t, err, "%+v", err)

}
