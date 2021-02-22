package executor

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/DataWorkbench/common/constants"
)

func JobParserFlink(client SourceClient, uclient UdfClient, depends string, flinkHome string, flinkExecJars string, nodeType int32) (err error, jobInfo JobParserInfo) {
	var (
		tablesName map[string]string
		ssql       constants.FlinkSSQL
		job        constants.FlinkJob
		title      string
	)

	// conf
	if nodeType == constants.NodeTypeFlinkSSQL {
		jobInfo.ZeppelinConf += "%flink.conf\n\n"
		jobInfo.ZeppelinConf += "FLINK_HOME " + flinkHome + "\n"
		jobInfo.ZeppelinConf += "flink.execution.mode remote\n"
		jobInfo.ZeppelinConf += "flink.execution.remote.host	" + FlinkHostQuote + "\n"
		jobInfo.ZeppelinConf += "flink.execution.remote.port	" + FlinkPortQuote + "\n"
		jobInfo.ZeppelinConf += "zeppelin.flink.concurrentBatchSql.max 1000000\nzeppelin.flink.concurrentStreamSql.max 1000000\n"
	} else if nodeType == constants.NodeTypeFlinkJob {
		jobInfo.ZeppelinConf += "%sh.conf\n\n"
		jobInfo.ZeppelinConf += "shell.command.timeout.millisecs    315360000000" // 1000×60×60×24×365×10 10years
	} else {
		err = fmt.Errorf("flink don't support the job type %d", nodeType)
		return
	}

	// depend
	if nodeType == constants.NodeTypeFlinkSSQL {
		var (
			property    string
			sourcetypes []string
			executeJars string
			udfJars     string
		)

		tablesName = make(map[string]string)
		if err = json.Unmarshal([]byte(depends), &ssql); err != nil {
			return
		}
		if ssql.Parallelism > 0 {
			property = "(parallelism=" + fmt.Sprintf("%d", ssql.Parallelism) + ")"
		} else {
			property = ""
		}
		title = "%flink.ssql" + property + "\n\n"
		jobInfo.ZeppelinDepends += title

		for _, table := range ssql.Tables {
			sourceID, tableName, tableUrl, errTmp := client.DescribeSourceTable(table)
			if errTmp != nil {
				err = errTmp
				return
			}
			sourceType, ManagerUrl, errTmp := client.DescribeSourceManager(sourceID)
			if errTmp != nil {
				err = errTmp
				return
			}

			if sourceType == constants.SourceTypeS3 {
				var m constants.SourceS3Params

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if jobInfo.S3info.AccessKey == "" {
					jobInfo.S3info.AccessKey = m.AccessKey
					jobInfo.S3info.SecretKey = m.SecretKey
					jobInfo.S3info.EndPoint = m.EndPoint
				} else if jobInfo.S3info.AccessKey != m.AccessKey || jobInfo.S3info.SecretKey != m.SecretKey || jobInfo.S3info.EndPoint != m.EndPoint {
					err = fmt.Errorf("only allow one s3 sourcemanger in a job, all accesskey secretkey endpoint is same")
					return
				}
			}

			find := false
			for _, save := range sourcetypes {
				if save == sourceType {
					find = true
					break
				}
			}
			if find == false {
				sourcetypes = append(sourcetypes, sourceType)
			}

			tablesName[table] = tableName
			jobInfo.ZeppelinDepends += "drop table if exists " + tableName + ";\n"
			jobInfo.ZeppelinDepends += "create table " + tableName + "\n"

			if sourceType == constants.SourceTypeMysql {
				var m constants.SourceMysqlParams
				var t constants.FlinkTableDefineMysql

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobInfo.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobInfo.ZeppelinDepends += column
						first = false
					} else {
						jobInfo.ZeppelinDepends += "," + column
					}
				}
				jobInfo.ZeppelinDepends += ") WITH (\n"
				jobInfo.ZeppelinDepends += "'connector' = 'jdbc',\n"
				jobInfo.ZeppelinDepends += "'url' = 'jdbc:" + "mysql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
				jobInfo.ZeppelinDepends += "'table-name' = '" + tableName + "',\n"
				jobInfo.ZeppelinDepends += "'username' = '" + m.User + "',\n"
				jobInfo.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
				for _, opt := range m.ConnectorOptions {
					jobInfo.ZeppelinDepends += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					jobInfo.ZeppelinDepends += "," + opt + "\n"
				}
			} else if sourceType == constants.SourceTypePostgreSQL {
				var m constants.SourcePostgreSQLParams
				var t constants.FlinkTableDefinePostgreSQL

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobInfo.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobInfo.ZeppelinDepends += column
						first = false
					} else {
						jobInfo.ZeppelinDepends += "," + column
					}
				}
				jobInfo.ZeppelinDepends += ") WITH (\n"
				jobInfo.ZeppelinDepends += "'connector' = 'jdbc',\n"
				jobInfo.ZeppelinDepends += "'url' = 'jdbc:" + "postgresql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
				jobInfo.ZeppelinDepends += "'table-name' = '" + tableName + "',\n"
				jobInfo.ZeppelinDepends += "'username' = '" + m.User + "',\n"
				jobInfo.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
				for _, opt := range m.ConnectorOptions {
					jobInfo.ZeppelinDepends += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					jobInfo.ZeppelinDepends += "," + opt + "\n"
				}
			} else if sourceType == constants.SourceTypeClickHouse {
				var m constants.SourceClickHouseParams
				var t constants.FlinkTableDefineClickHouse

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobInfo.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobInfo.ZeppelinDepends += column
						first = false
					} else {
						jobInfo.ZeppelinDepends += "," + column
					}
				}
				jobInfo.ZeppelinDepends += ") WITH (\n"
				jobInfo.ZeppelinDepends += "'connector' = 'clickhouse',\n"
				jobInfo.ZeppelinDepends += "'url' = 'clickhouse://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "',\n"
				jobInfo.ZeppelinDepends += "'table-name' = '" + tableName + "',\n"
				jobInfo.ZeppelinDepends += "'username' = '" + m.User + "',\n"
				jobInfo.ZeppelinDepends += "'database-name' = '" + m.Database + "',\n"
				jobInfo.ZeppelinDepends += "'password' = '" + m.Password + "'\n"
				for _, opt := range m.ConnectorOptions {
					jobInfo.ZeppelinDepends += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					jobInfo.ZeppelinDepends += "," + opt + "\n"
				}
			} else if sourceType == constants.SourceTypeKafka {
				var m constants.SourceKafkaParams
				var t constants.FlinkTableDefineKafka

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobInfo.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobInfo.ZeppelinDepends += column
						first = false
					} else {
						jobInfo.ZeppelinDepends += "," + column
					}
				}
				jobInfo.ZeppelinDepends += ") WITH (\n"

				jobInfo.ZeppelinDepends += "'connector' = 'kafka',\n"
				jobInfo.ZeppelinDepends += "'topic' = '" + t.Topic + "',\n"
				jobInfo.ZeppelinDepends += "'properties.bootstrap.servers' = '" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "',\n"
				jobInfo.ZeppelinDepends += "'format' = '" + t.Format + "'\n"
				for _, opt := range m.ConnectorOptions {
					jobInfo.ZeppelinDepends += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					jobInfo.ZeppelinDepends += "," + opt + "\n"
				}
			} else if sourceType == constants.SourceTypeS3 {
				var m constants.SourceS3Params
				var t constants.FlinkTableDefineS3

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				jobInfo.ZeppelinDepends += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						jobInfo.ZeppelinDepends += column
						first = false
					} else {
						jobInfo.ZeppelinDepends += "," + column
					}
				}
				jobInfo.ZeppelinDepends += ") WITH (\n"

				jobInfo.ZeppelinDepends += "'connector' = 'filesystem',\n"
				jobInfo.ZeppelinDepends += "'path' = '" + t.Path + "',\n"
				jobInfo.ZeppelinDepends += "'format' = '" + t.Format + "'\n"
				for _, opt := range t.ConnectorOptions {
					jobInfo.ZeppelinDepends += "," + opt + "\n"
				}
			} else {
				err = fmt.Errorf("don't support this source mananger %s", sourceType)
				return
			}

			jobInfo.ZeppelinDepends += ");\n\n\n"
		}

		for _, jar := range strings.Split(strings.Replace(flinkExecJars, " ", "", -1), ";") {
			sourceType := strings.Split(jar, ":")[0]
			executeJar := strings.Split(jar, ":")[1]

			for _, jobSourceType := range sourcetypes {
				if sourceType == jobSourceType {
					if len(executeJars) > 0 {
						executeJars += ","
					}
					executeJars += executeJar
				}
			}
		}
		jobInfo.ZeppelinConf += "flink.execution.jars " + executeJars + "\n"

		//main run
		jobInfo.ZeppelinMainRun += title
		jobInfo.ZeppelinMainRun += ssql.MainRun
		for table, tableName := range tablesName {
			jobInfo.ZeppelinMainRun = strings.Replace(jobInfo.ZeppelinMainRun, Quote+table+Quote, tableName, -1)
		}
		jobInfo.ZeppelinMainRun += "\n"

		//udf
		for _, udfId := range ssql.Funcs {
			udfType, udfName, define, errTmp := uclient.DescribeUdfManager(udfId)
			if errTmp != nil {
				err = errTmp
				return
			}
			if udfType == constants.UdfTypeScala {
				if jobInfo.ZeppelinFuncScala == "" {
					jobInfo.ZeppelinFuncScala = "%flink\n\n"
				}
				jobInfo.ZeppelinFuncScala += strings.Replace(define, FlinkUDFNAMEQuote, `\"`+udfName+`\"`, -1) + "\n\n\n"
			} else if udfType == constants.UdfTypeJar {
				if udfJars != "" {
					udfJars += ","
				}
				udfJars += strings.Replace(define, " ", "", -1)
				//TODO download and other
			} else {
				err = fmt.Errorf("don't support the udf type %s", udfType)
				return
			}
			jobInfo.ZeppelinMainRun = strings.ReplaceAll(jobInfo.ZeppelinMainRun, Quote+udfId+Quote, udfName)
		}
		if udfJars != "" {
			jobInfo.ZeppelinConf += "flink.udf.jars " + udfJars + "\n"
		}
	} else if nodeType == constants.NodeTypeFlinkJob {
		var (
			entry          string
			jarParallelism string
			checkv         = regexp.MustCompile(`^[a-zA-Z0-9_/. ]*$`).MatchString
		)

		if err = json.Unmarshal([]byte(depends), &job); err != nil {
			return
		}

		if checkv(job.JarArgs) == false {
			err = fmt.Errorf("only ^[a-zA-Z0-9_/. ]*$ is allow in jarargs")
			return
		}
		if checkv(job.JarEntry) == false {
			err = fmt.Errorf("only ^[a-zA-Z0-9_/. ]*$ is allow in jarentry")
			return
		}
		title = "%sh\n\n"
		jobInfo.ZeppelinMainRun += title

		if len(job.JarEntry) > 0 {
			entry = ""
		} else {
			entry = " -c '" + job.JarEntry + "' "
		}

		if job.Parallelism > 0 {
			jarParallelism = " -p " + fmt.Sprintf("%d", job.Parallelism) + " "
		} else {
			jarParallelism = ""
		}
		//TODO download
		jobInfo.ZeppelinMainRun += flinkHome + "/bin/flink run -sae -m " + FlinkHostQuote + ":" + FlinkPortQuote + jarParallelism + entry + job.MainRun + " " + job.JarArgs
		jobInfo.resources.Jar = job.MainRun
	}
	return
}
