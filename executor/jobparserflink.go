package executor

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/DataWorkbench/common/constants"
)

func GenerateFlinkConf(flinkHome string, flinkExecJars string, engineHost string, enginePort string, nodeType int32) (o string) {
	if nodeType == constants.NodeTypeFlinkSSQL {
		title := "%flink.conf\n\n"
		home := "FLINK_HOME " + flinkHome + "\n"
		mode := "flink.execution.mode remote\n"
		host := "flink.execution.remote.host	" + engineHost + "\n"
		port := "flink.execution.remote.port	" + enginePort + "\n"
		jars := "flink.execution.jars " + flinkExecJars + "\n"
		others := "zeppelin.flink.concurrentBatchSql.max 1000000\nzeppelin.flink.concurrentStreamSql.max 1000000\n"

		return title + home + mode + host + port + jars + others
	} else if nodeType == constants.NodeTypeFlinkJob {
		title := "%sh.conf\n\n"
		timeOut := "shell.command.timeout.millisecs    315360000000" // 1000×60×60×24×365×10 10years
		return title + timeOut
	}
	return
}

func GenerateFlinkJob(client SourceClient, flinkHome string, flinkAddr string, nodeType int32, depends string) (dependsText string, mainRunText string, resources JobResources, err error) {
	var (
		title      string
		tablesName map[string]string
		ssql       constants.FlinkSSQL
		job        constants.FlinkJob
	)

	tablesName = make(map[string]string)

	if nodeType == constants.NodeTypeFlinkSSQL {
		var property string
		if err = json.Unmarshal([]byte(depends), &ssql); err != nil {
			return
		}
		if ssql.Parallelism > 0 {
			property = "(parallelism=" + fmt.Sprintf("%d", ssql.Parallelism) + ")"
		} else {
			property = ""
		}

		title = "%flink.ssql" + property + "\n\n"
	} else if nodeType == constants.NodeTypeFlinkJob {
		var checkv = regexp.MustCompile(`^[a-zA-Z0-9_/. ]*$`).MatchString

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
	} else {
		err = fmt.Errorf("flink don't support the job type %d", nodeType)
		return
	}

	dependsText += title
	if nodeType == constants.NodeTypeFlinkSSQL {
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

			tablesName[table] = tableName
			dependsText += "drop table if exists " + tableName + ";\n"
			dependsText += "create table " + tableName + "\n"

			if sourceType == constants.SourceTypeMysql {
				var m constants.SourceMysqlParams
				var t constants.FlinkTableDefineMysql

				if err = json.Unmarshal([]byte(ManagerUrl), &m); err != nil {
					return
				}
				if err = json.Unmarshal([]byte(tableUrl), &t); err != nil {
					return
				}
				dependsText += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						dependsText += column
						first = false
					} else {
						dependsText += "," + column
					}
				}
				dependsText += ") WITH (\n"
				dependsText += "'connector' = 'jdbc',\n"
				dependsText += "'url' = 'jdbc:" + "mysql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
				dependsText += "'table-name' = '" + tableName + "',\n"
				dependsText += "'username' = '" + m.User + "',\n"
				dependsText += "'password' = '" + m.Password + "'\n"
				for _, opt := range m.ConnectorOptions {
					dependsText += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					dependsText += "," + opt + "\n"
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
				dependsText += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						dependsText += column
						first = false
					} else {
						dependsText += "," + column
					}
				}
				dependsText += ") WITH (\n"
				dependsText += "'connector' = 'jdbc',\n"
				dependsText += "'url' = 'jdbc:" + "postgresql" + "://" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "/" + m.Database + "',\n"
				dependsText += "'table-name' = '" + tableName + "',\n"
				dependsText += "'username' = '" + m.User + "',\n"
				dependsText += "'password' = '" + m.Password + "'\n"
				for _, opt := range m.ConnectorOptions {
					dependsText += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					dependsText += "," + opt + "\n"
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
				dependsText += "("
				first := true
				for _, column := range t.SqlColumn {
					if first == true {
						dependsText += column
						first = false
					} else {
						dependsText += "," + column
					}
				}
				dependsText += ") WITH (\n"

				dependsText += "'connector' = 'kafka',\n"
				dependsText += "'topic' = '" + t.Topic + "',\n"
				dependsText += "'properties.bootstrap.servers' = '" + m.Host + ":" + fmt.Sprintf("%d", m.Port) + "',\n"
				dependsText += "'format' = '" + t.Format + "'\n"
				for _, opt := range m.ConnectorOptions {
					dependsText += "," + opt + "\n"
				}
				for _, opt := range t.ConnectorOptions {
					dependsText += "," + opt + "\n"
				}
			} else {
				err = fmt.Errorf("don't support this source mananger %s", sourceType)
				return
			}

			dependsText += ");\n\n\n"
		}
	} else if nodeType == constants.NodeTypeFlinkJob {
		dependsText += "ls" //empty is not allow.
	}

	// main run
	mainRunText += title
	if nodeType == constants.NodeTypeFlinkSSQL {
		mainRunText += ssql.MainRun
		for table, tableName := range tablesName {
			mainRunText = strings.Replace(mainRunText, constants.MainRunQuote+table+constants.MainRunQuote, tableName, -1)
		}
		mainRunText += "\n"
	} else if nodeType == constants.NodeTypeFlinkJob {
		var (
			entry          string
			jarParallelism string
		)

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
		mainRunText += flinkHome + "/bin/flink run -m " + flinkAddr + jarParallelism + entry + job.MainRun + " " + job.JarArgs
		resources.Jar = job.MainRun
	}

	return
}
