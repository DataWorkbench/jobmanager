package executor

import (
	"fmt"
)

func GetEngine(options string) (engineType string, engineHost string, enginePort string, engineOption string, err error) {
	engineType = "Flink"
	//engineHost = "127.0.0.1"
	engineHost = "flinkjobmanager"
	enginePort = "8081"
	engineOption = ""
	fmt.Println("get engine")
	return
}

func FreeEngine(jobID string) {
	fmt.Println("free engine")
}
