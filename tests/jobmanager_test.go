package tests
//
//import (
//	"context"
//	"encoding/json"
//	"testing"
//
//	"github.com/DataWorkbench/glog"
//	"github.com/stretchr/testify/require"
//
//	"github.com/DataWorkbench/common/grpcwrap"
//	"github.com/DataWorkbench/common/utils/idgenerator"
//
//	"bytes"
//	"crypto/rand"
//	"math/big"
//
//	"gorm.io/driver/mysql"
//	"gorm.io/gorm"
//
//	"github.com/DataWorkbench/gproto/pkg/flinkpb"
//	"github.com/DataWorkbench/gproto/pkg/jobpb"
//	"github.com/DataWorkbench/gproto/pkg/model"
//	"github.com/DataWorkbench/gproto/pkg/request"
//)
//
//// from mysql to mysql
//var msmd request.JobInfo //regress test
//
//// from kafka to kafka
//var kafka_source_dest request.JobInfo
//
//// from hdfs to hdfs
//var hdfs_source_dest request.JobInfo
//
//// from s3 to s3
//var s3_source_dest request.JobInfo
//
//// from hbase to hbase
//var hbase_source_dest request.JobInfo
//
//// from pg to pg
//var pg_source_dest request.JobInfo
//
//// from ck to ck
//var ck_source_dest request.JobInfo
//
//var preview_ms request.JobInfo   //regress test
//var sql_msmd request.JobInfo     //regress test
//var python_print request.JobInfo //regress test
//var scala_print request.JobInfo  //regress test
//
//func CreateRandomString(len int) string {
//	var container string
//	var str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
//	b := bytes.NewBufferString(str)
//	length := b.Len()
//	bigInt := big.NewInt(int64(length))
//	for i := 0; i < len; i++ {
//		randomInt, _ := rand.Int(rand.Reader, bigInt)
//		container += string(str[randomInt.Int64()])
//	}
//	return container
//}
//
//func typeToJsonString(v interface{}) string {
//	s, _ := json.Marshal(&v)
//	return string(s)
//}
//
//var client jobpb.JobmanagerClient
//var ctx context.Context
//var spaceid string
//
//func mainInit(t *testing.T, manualInit bool) {
//	// create table in mysql
//	dsn := "root:password@tcp(127.0.0.1:3306)/data_workbench"
//	db, cerr := gorm.Open(mysql.Open(dsn), &gorm.Config{})
//	require.Nil(t, cerr, "%+v", cerr)
//	db.Exec("CREATE TABLE IF NOT EXISTS ms(id bigint, id1 bigint)")
//	db.Exec("CREATE TABLE IF NOT EXISTS md(id bigint, id1 bigint)")
//	db.Exec("delete from ms")
//	db.Exec("delete from md")
//	db.Exec("insert into ms values(1, 1)")
//	db.Exec("insert into ms values(2, 2)")
//	db.Exec("insert into workspace values('wks-0123456789012345', 'usr-ndcbIwIa', 'lzz', 'lzz', 1, now(), now());")
//	if manualInit == true {
//		db.Exec("CREATE TABLE IF NOT EXISTS mw(rate bigint, dbmoney varchar(10))")
//		db.Exec("CREATE TABLE IF NOT EXISTS mwd(total bigint)")
//		db.Exec("insert into mw values(2, 'EUR')")
//		db.Exec("insert into mw values(7, 'USD')")
//	}
//	spaceid = "wks-0000000000000001"
//
//	sql_msmd = request.JobInfo{JobId: "job-0000000000000sql", SpaceId: spaceid, Args: &model.StreamJobArgs{ClusterId: "eng-0000000000000000"}, Code: &model.StreamJobCode{Type: model.StreamJob_SQL, Sql: &flinkpb.FlinkSQL{Code: "drop table if exists md;\ncreate table md\n(id bigint,id1 bigint) WITH (\n'connector' = 'jdbc',\n'url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench',\n'table-name' = 'md',\n'username' = 'root',\n'password' = 'password'\n);\n\n\ndrop table if exists ms;\ncreate table ms\n(id bigint,id1 bigint) WITH (\n'connector' = 'jdbc',\n'url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench',\n'table-name' = 'ms',\n'username' = 'root',\n'password' = 'password'\n);\n\n\ninsert into md(id,id1) select ALL id as id,id1 from ms \n"}}}
//	//msmd = request.JobInfo{JobId: "job-00000source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-00000mysqlsource", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-0000000mysqldest", Columns: []string{"id", "id1"}}}}}}}
//	//msmd = request.JobInfo{JobId: "job-00000source_dest", SpaceId: spaceid, Args: &model.StreamJobArgs{Function: &model.StreamJobArgs_Function{UdfIds: []string{"udf-0000scalaplusone"}}}, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-00000mysqlsource", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "plus_one(id)"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-0000000mysqldest", Columns: []string{"id", "id1"}}}}}}}
//	preview_ms = request.JobInfo{JobId: "job-00000source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-00000mysqlsource", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}}}}
//	python_print = request.JobInfo{JobId: "job-0000000000python", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Python, Python: &flinkpb.FlinkPython{Code: "print(\"hello world\")"}}}
//	scala_print = request.JobInfo{JobId: "job-00000000000scala", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Scala, Scala: &flinkpb.FlinkScala{Code: "println(\"hello world\")"}}}
//	kafka_source_dest = request.JobInfo{JobId: "job-kafkasource_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-00000kafkasource", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "paycount"}, &flinkpb.ColumnAs{Field: "paymoney"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-0000000kafkadest", Columns: []string{"paycount", "paymoney"}}}}}}}
//	hdfs_source_dest = request.JobInfo{JobId: "job-hdfs_source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-00000hdfs_source", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-0000000hdfs_dest", Columns: []string{"id", "id1"}}}}}}}
//	s3_source_dest = request.JobInfo{JobId: "job-00s3_source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-0000000s3_source", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-000000000s3_dest", Columns: []string{"id", "id1"}}}}}}}
//	hbase_source_dest = request.JobInfo{JobId: "job-hbasesource_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-0000hbase_source", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "rowkey"}, &flinkpb.ColumnAs{Field: "columna"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-000000hbase_dest", Columns: []string{"rowkey", "columna"}}}}}}}
//	pg_source_dest = request.JobInfo{JobId: "job-00pg_source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-0postgres_source", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-000postgres_dest", Columns: []string{"id", "id1"}}}}}}}
//	ck_source_dest = request.JobInfo{JobId: "job-00ck_source_dest", SpaceId: spaceid, Code: &model.StreamJobCode{Type: model.StreamJob_Operator, Operators: []*flinkpb.FlinkOperator{&flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Source, Id: "xx0", DownStream: "xx1", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Source: &flinkpb.SourceOperator{TableId: "sot-clickhousesource", Column: []*flinkpb.ColumnAs{&flinkpb.ColumnAs{Field: "id"}, &flinkpb.ColumnAs{Field: "id1"}}}}}, &flinkpb.FlinkOperator{Type: flinkpb.FlinkOperator_Dest, Id: "xx1", Upstream: "xx0", PointX: 1, PointY: 1, Property: &flinkpb.OperatorProperty{Dest: &flinkpb.DestOperator{TableId: "sot-0clickhouse_dest", Columns: []string{"id", "id1"}}}}}}}
//
//	//mw = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012346","table":"billing as k","distinct":"ALL","column":[{"field":"k.paycount * r.rate","as":"stotal"}]}},{"nodetype":"Source","nodeid":"rxx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012355","table":"mw FOR SYSTEM_TIME AS OF k.tproctime AS r","distinct":"ALL","column":null}},{"nodetype":"Join","nodeid":"xx1","upstream":"xx0","upstreamright":"rxx0","downstream":"xx2","pointx":"","pointy":"","property":{"join":"JOIN","expression":"r.dbmoney = k.paymoney","column":[{"field":"stotal","as":""}]}},{"nodetype":"Dest","nodeid":"xx2","upstream":"xx1","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"mwd","column":["total"],"id":"sot-0123456789012356"}}]}`}
//	////{"paycount": 2, "paymoney": "EUR"} {"paycount": 1, "paymoney": "USD"}
//	//mspdcancel = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"id1","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
//	//mspdcancelall1 = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"id1","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
//	//mspdcancelall2 = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"id1","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
//	//udf = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012347","table":"ms","distinct":"ALL","column":[{"field":"id","as":"id"},{"field":"javatwice(id1)","as":"id1"}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"pd","column":["id","id1"],"id":"sot-0123456789012348"}}]}`}
//	//jar = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":false,"env":{"engine_id":"","parallelism":0,"jobcu":2,"taskcu":2,"tasknum":2,"custom":null},"nodes":[{"nodetype":"Jar","nodeid":"xxxx","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"jar_args":"","jar_entry":"spendreport.FraudDetectionJob","jar_id":"fil-04bbca8755d62131","accesskey":"","secretkey":"","endpoint":"","hbasehosts":""}}]}`}
//	//ftp = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012366","table":"ftpsource","distinct":"ALL","column":[{"field":"readName","as":"readName"},{"field":"cellPhone","as":"cellPhone"},{"field":"universityName","as":"universityName"},{"field":"city","as":"city"},{"field":"street","as":"street"},{"field":"ip","as":"ip"}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"ftpdest","column":["readName","cellPhone","universityName","city","street","ip"],"id":"sot-0123456789012367"}}]}`}
//
//	////s3 = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012359", "sot-0123456789012360"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012360$qc$ select * from $qc$sot-0123456789012359$qc$"})}
//	////ck = jobpb.RunJobRequest{ID: CreateRandomString(20), WorkspaceID: spaceID45, NodeType: constants.NodeTypeFlinkSSQL, Depends: typeToJsonString(constants.FlinkSSQL{Tables: []string{"sot-0123456789012361"}, Parallelism: 2, MainRun: "insert into $qc$sot-0123456789012361$qc$ values(6, 6)"})}
//	//SqlJobAndUdf = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Sql","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"sql":"\n\ndrop table if exists pd;\ncreate table pd\n(id bigint , id1 bigint COMMENT 'xxx' , PRIMARY KEY (id) NOT ENFORCED) WITH (\n'connector' = 'jdbc',\n'url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench',\n'table-name' = 'pd',\n'username' = 'root',\n'password' = 'password'\n);\n\n\ndrop table if exists ms;\ncreate table ms\n(id bigint , id1 bigint COMMENT 'xxx' , PRIMARY KEY (id) NOT ENFORCED) WITH (\n'connector' = 'jdbc',\n'url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench',\n'table-name' = 'ms',\n'username' = 'root',\n'password' = 'password'\n);\n\n\n\n\ninsert into pd(id,id1)  select ALL plus_one(id) as id, ascii(python_upper('a')) from ms "}}]}`}
//	////insert into pd(id,id1)  select ALL id as id,id1 from ms
//	//// = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: }
//	//Hbase = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Source","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"xx1","pointx":"","pointy":"","property":{"id":"sot-0123456789012364","table":"testsource","distinct":"ALL","column":[{"field":"rowkey","as":""},{"field":"columna","as":""}]}},{"nodetype":"Dest","nodeid":"xx1","upstream":"xx0","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"table":"testdest","column":["rowkey","columna"],"id":"sot-0123456789012365"}}]}`}
//	//PythonTable = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Python","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"code":"from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic \nfrom pyflink.table import StreamTableEnvironment, EnvironmentSettings \ndef insertselect():\n    t_env = StreamTableEnvironment.create(stream_execution_environment=s_env)\n    source_ddl = \"create table ms(id bigint , id1 bigint COMMENT 'xxx' , PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench','table-name' = 'ms','username' = 'root','password' = 'password')\" \n    sink_ddl =   \"create table pd(id bigint , id1 bigint COMMENT 'xxx' , PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://dataworkbench-db:3306/data_workbench','table-name' = 'pd','username' = 'root','password' = 'password')\" \n    t_env.execute_sql(source_ddl) \n    t_env.execute_sql(sink_ddl) \n    t_env.sql_query(\"SELECT * FROM ms\").insert_into(\"pd\") \n    t_env.execute(\"demo\") \nif __name__ == '__main__':\n    insertselect()"}}]}`}
//	//PythonPrint = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Python","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"code":"print('hello world')"}}]}`}
//	//ScalaPrint = jobpb.RunJobRequest{ID: CreateRandomString(20), SpaceID: spaceID45, EngineID: CreateRandomString(20), EngineType: constants.EngineTypeFlink, JobInfo: `{"stream_sql":true,"env":{"engine_id":"","parallelism":2,"job_mem":0,"job_cpu":0,"task_cpu":0,"task_mem":0,"task_num":0,"custom":null},"nodes":[{"nodetype":"Scala","nodeid":"xx0","upstream":"","upstreamright":"","downstream":"","pointx":"","pointy":"","property":{"code":"println(\"hello world\")"}}]}`}
//
//	address := "127.0.0.1:9105"
//	lp := glog.NewDefault()
//	ctx = glog.WithContext(context.Background(), lp)
//
//	conn, err := grpcwrap.NewConn(ctx, &grpcwrap.ClientConfig{
//		Address: address,
//	})
//	require.Nil(t, err, "%+v", err)
//	client = jobpb.NewJobmanagerClient(conn)
//
//	logger := glog.NewDefault()
//	worker := idgenerator.New("")
//	reqId, _ := worker.Take()
//
//	ln := logger.Clone()
//	ln.WithFields().AddString("rid", reqId)
//
//	ctx = grpcwrap.ContextWithRequest(context.Background(), ln, reqId)
//}
//
//func Test_Run(t *testing.T) {
//	mainInit(t, false)
//	var err error
//
//	//_, err = client.Run(ctx, &sql_msmd)
//	//require.Nil(t, err, "%+v", err)
//	_, err = client.Run(ctx, &msmd)
//	require.Nil(t, err, "%+v", err)
//	//_, err = client.Run(ctx, &python_print)
//	//require.Nil(t, err, "%+v", err)
//	//_, err = client.Run(ctx, &scala_print)
//	//require.Nil(t, err, "%+v", err)
//	//_, err = client.Run(ctx, &kafka_source_dest)
//	//require.Nil(t, err, "%+v", err)
//	//_, err = client.Run(ctx, &hdfs_source_dest)
//	//require.Nil(t, err, "%+v", err)
//	//_, err = client.Run(ctx, &s3_source_dest)
//	//require.Nil(t, err, "%+v", err)
//	//_, err = client.Run(ctx, &hbase_source_dest)
//	//require.Nil(t, err, "%+v", err)
//	//_, err = client.Run(ctx, &pg_source_dest)
//	//require.Nil(t, err, "%+v", err)
//	//_, err = client.Run(ctx, &ck_source_dest)
//	//require.Nil(t, err, "%+v", err)
//}
//
//func Test_GetState(t *testing.T) {
//	mainInit(t, false)
//	var err error
//
//	//req := request.JobGetState{JobId: ck_source_dest.JobId}
//	//_, err = client.GetState(ctx, &req)
//	//require.Nil(t, err, "%+v", err)
//
//	//req := request.JobGetState{JobId: pg_source_dest.JobId}
//	//_, err = client.GetState(ctx, &req)
//	//require.Nil(t, err, "%+v", err)
//
//	//req := request.JobGetState{JobId: hbase_source_dest.JobId}
//	//_, err = client.GetState(ctx, &req)
//	//require.Nil(t, err, "%+v", err)
//
//	//req := request.JobGetState{JobId: s3_source_dest.JobId}
//	//_, err = client.GetState(ctx, &req)
//	//require.Nil(t, err, "%+v", err)
//
//	//req := request.JobGetState{JobId: hdfs_source_dest.JobId}
//	//_, err = client.GetState(ctx, &req)
//	//require.Nil(t, err, "%+v", err)
//
//	//req := request.JobGetState{JobId: kafka_source_dest.JobId}
//	//_, err = client.GetState(ctx, &req)
//	//require.Nil(t, err, "%+v", err)
//
//	req := request.JobGetState{JobId: msmd.JobId}
//	_, err = client.GetState(ctx, &req)
//	require.Nil(t, err, "%+v", err)
//
//	//req = request.JobGetState{JobId: sql_msmd.JobId}
//	//_, err = client.GetState(ctx, &req)
//	//require.Nil(t, err, "%+v", err)
//	//require.Equal(t, "success", "failed")
//
//	//req := request.JobGetState{JobId: python_print.JobId}
//	//_, err = client.GetState(ctx, &req)
//	//require.Nil(t, err, "%+v", err)
//
//	//req = request.JobGetState{JobId: scala_print.JobId}
//	//_, err = client.GetState(ctx, &req)
//	//require.Nil(t, err, "%+v", err)
//}
//
//func Test_OperatorRelations(t *testing.T) {
//	mainInit(t, false)
//
//	var req model.EmptyStruct
//
//	_, err := client.NodeRelations(ctx, &req)
//	require.Nil(t, err, "%+v", err)
//	//require.Equal(t, "", resp.Relations)
//}
//
//func Test_Preview(t *testing.T) {
//	mainInit(t, false)
//
//	_, err := client.Preview(ctx, &msmd)
//	require.Nil(t, err, "%+v", err)
//	_, err = client.Preview(ctx, &preview_ms)
//	require.Nil(t, err, "%+v", err)
//}
//
//func Test_Syntax(t *testing.T) {
//	mainInit(t, false)
//
//	//_, err := client.Syntax(ctx, &msmd)
//	//require.Nil(t, err, "%+v", err)
//	_, err := client.Syntax(ctx, &sql_msmd)
//	require.Nil(t, err, "%+v", err)
//}
//
//func Test_CancelJob(t *testing.T) {
//	var req request.JobCancel
//	var err error
//
//	mainInit(t, false)
//
//	req.JobId = kafka_source_dest.JobId
//	_, err = client.CancelJob(ctx, &req)
//	require.Nil(t, err, "%+v", err)
//}
//
////func Test_CancelAllJob(t *testing.T) {
////	var req jobpb.CancelAllJobRequest
////	var err error
//// TODO
////
////	mainInit(t, false)
////
////	req.SpaceID = spaceID45
////	time.Sleep(time.Second * 10)
////
////	go client.Run(ctx, &mspdcancelall1)
////	go client.Run(ctx, &mspdcancelall2)
////
////	time.Sleep(time.Second * 10)
////	_, err = client.CancelAllJob(ctx, &req)
////	require.Nil(t, err, "%+v", err)
////}
