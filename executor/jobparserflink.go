package executor

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	SourceTypeMysql      = "MySQL"
	SourceTypePostgreSQL = "PostgreSQL"
	SourceTypeKafka      = "Kafka"
	workbenchQuote       = "$qc$"

	dependTable       = "table"
	dependFunc        = "func"
	dependParallelism = "parallelism"
	dependJobCpu      = "jobcpu"
	dependJobMem      = "jobmem"
	dependTaskCpu     = "taskcpu"
	dependTaskMem     = "taskmem"
	dependTaskNum     = "tasknum"
	dependJarArgs     = "jarargs"
	dependJarEntry    = "jarentry"
)

func GenerateFlinkConf(flinkHome string, flinkExecJars string, engineHost string, enginePort string, nodeType string) (o string) {
	if nodeType == FlinkJobSsql {
		title := "%flink.conf\n\n"
		home := "FLINK_HOME " + flinkHome + "\n"
		mode := "flink.execution.mode remote\n"
		host := "flink.execution.remote.host	" + engineHost + "\n"
		port := "flink.execution.remote.port	" + enginePort + "\n"
		jars := "flink.execution.jars " + flinkExecJars + "\n"

		return title + home + mode + host + port + jars
	} else if nodeType == FlinkJobJar {
		title := "%sh.conf\n\n"
		timeOut := "shell.command.timeout.millisecs    315360000000" // 1000×60×60×24×365×10 10years
		return title + timeOut
	}
	return
}

func GenerateFlinkJob(client SourceClient, flinkHome string, flinkAddr string, nodeType string, depends string, mainRun string) (dependsText string, mainRunText string, resources string, err error) {
	var (
		title       string
		property    string
		tablesName  map[string]string
		dependsJson map[string]string
	)

	tablesName = make(map[string]string)

	if err = json.Unmarshal([]byte(depends), &dependsJson); err != nil {
		return
	}

	if nodeType == FlinkJobSsql {
		if _, ok := dependsJson[dependTable]; ok == false {
			err = fmt.Errorf("can't find " + dependTable + " in depends")
			return
		}
		if _, ok := dependsJson[dependFunc]; ok == false {
			err = fmt.Errorf("can't find " + dependFunc + " in depends")
			return
		}
		if _, ok := dependsJson[dependParallelism]; ok == false {
			err = fmt.Errorf("can't find " + dependParallelism + " in depends")
			return
		}
		if dependsJson[dependParallelism] == "0" || dependsJson[dependParallelism] == "" {
			property = ""
		} else {
			property = "(parallelism=" + dependsJson[dependParallelism] + ")"
		}

		title = "%flink.ssql" + property + "\n\n"
	} else if nodeType == "jar" {
		/*
			if _, ok := dependsJson[dependFunc]; ok == false {
				err = fmt.Errorf("can't find " + dependFunc + " in depends")
				return
			}
		*/
		if _, ok := dependsJson[dependParallelism]; ok == false {
			err = fmt.Errorf("can't find " + dependParallelism + " in depends")
			return
		}

		if _, ok := dependsJson[dependJarArgs]; ok == false {
			err = fmt.Errorf("can't find " + dependJarArgs + " in depends")
			return
		}

		if _, ok := dependsJson[dependJarEntry]; ok == false {
			err = fmt.Errorf("can't find " + dependJarEntry + " in depends")
			return
		}

		title = "%sh\n\n"
	} else {
		err = fmt.Errorf("flink don't support the job type %s", nodeType)
		return
	}

	dependsText += title //if don't have any thing,  the title will run success.
	if nodeType == FlinkJobSsql {
		// table
		tablesID := strings.Split(strings.Replace(dependsJson[dependTable], " ", "", -1), ",")
		for _, table := range tablesID {
			sourceID, tableName, tableUrl, errTmp := client.DescribeSourceTable(table)
			if errTmp != nil {
				err = errTmp
				return
			}

			var tableUrlJson map[string]string
			if err = json.Unmarshal([]byte(tableUrl), &tableUrlJson); err != nil {
				return
			}
			tablesName[table] = tableName
			dependsText += "drop table if exists " + tableName + ";\n"
			dependsText += "create table " + tableName + "\n"
			dependsText += tableUrlJson["sqlColumn"] + " WITH (\n"

			sourceType, ManagerUrl, errTmp := client.DescribeSourceManager(sourceID)
			if errTmp != nil {
				err = errTmp
				return
			}
			var ManagerUrlJson map[string]string
			if err = json.Unmarshal([]byte(ManagerUrl), &ManagerUrlJson); err != nil {
				return
			}
			if sourceType == SourceTypeMysql {
				dependsText += "'connector' = 'jdbc',\n"
				dependsText += "'url' = 'jdbc:" + "mysql" + "://" + ManagerUrlJson["host"] + ":" + ManagerUrlJson["port"] + "/" + ManagerUrlJson["database"] + "',\n"
				dependsText += "'table-name' = '" + tableName + "',\n"
				dependsText += "'username' = '" + ManagerUrlJson["user"] + "',\n"
				dependsText += "'password' = '" + ManagerUrlJson["password"] + "'\n"
				if ManagerUrlJson["connector_options"] != "" {
					dependsText += "," + ManagerUrlJson["connector_options"] + "\n"
				}
				if tableUrlJson["connector_options"] != "" {
					dependsText += "," + tableUrlJson["connector_options"] + "\n"
				}
			} else if sourceType == SourceTypePostgreSQL {
				dependsText += "'connector' = 'jdbc',\n"
				dependsText += "'url' = 'jdbc:" + "postgresql" + "://" + ManagerUrlJson["host"] + ":" + ManagerUrlJson["port"] + "/" + ManagerUrlJson["database"] + "',\n"
				dependsText += "'table-name' = '" + tableName + "',\n"
				dependsText += "'username' = '" + ManagerUrlJson["user"] + "',\n"
				dependsText += "'password' = '" + ManagerUrlJson["password"] + "'\n"
				if ManagerUrlJson["connector_options"] != "" {
					dependsText += "," + ManagerUrlJson["connector_options"] + "\n"
				}
				if tableUrlJson["connector_options"] != "" {
					dependsText += "," + tableUrlJson["connector_options"] + "\n"
				}
			} else if sourceType == SourceTypeKafka {
				dependsText += "'connector' = 'kafka',\n"
				dependsText += "'topic' = '" + tableUrlJson["topic"] + "',\n"
				dependsText += "'properties.bootstrap.servers' = '" + ManagerUrlJson["host"] + ":" + ManagerUrlJson["port"] + "',\n"
				dependsText += "'properties.group.id' = '" + tableUrlJson["groupid"] + "',\n"
				dependsText += "'format' = '" + tableUrlJson["format"] + "'\n"
				if ManagerUrlJson["connector_options"] != "" {
					dependsText += "," + ManagerUrlJson["connector_options"] + "\n"
				}
				if tableUrlJson["connector_options"] != "" {
					dependsText += "," + tableUrlJson["connector_options"] + "\n"
				}
			} else {
				err = fmt.Errorf("don't support this source mananger %s", sourceType)
				return
			}

			dependsText += ");\n\n\n"
		}
	} else if nodeType == FlinkJobJar {
		dependsText += "ls" //empty is not allow.
	}

	// main run
	mainRunText += title
	if nodeType == FlinkJobSsql {
		mainRunText += mainRun
		for table, tableName := range tablesName {
			mainRunText = strings.Replace(mainRunText, workbenchQuote+table+workbenchQuote, tableName, -1)
		}
		mainRunText += "\n"
		resources = "{}"
	} else if nodeType == FlinkJobJar {
		var (
			entry          string
			jarParallelism string
		)

		if dependsJson[dependJarEntry] == "" {
			entry = ""
		} else {
			entry = " -c " + dependsJson[dependJarEntry] + " "
		}

		if dependsJson[dependParallelism] == "0" || dependsJson[dependParallelism] == "" {
			jarParallelism = ""
		} else {
			jarParallelism = " -p " + dependsJson[dependParallelism] + " "
		}
		//TODO download
		mainRunText += flinkHome + "/bin/flink run -m " + flinkAddr + jarParallelism + entry + mainRun + " " + dependsJson[dependJarArgs]

		resources = `{"` + FlinkJobJar + `": "` + mainRun + `"}`
	}

	return
}
