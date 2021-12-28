package executor

import (
	"fmt"
	"testing"
)

func Test_WaitUtilRunning(t *testing.T) {
	var (
		zeppelinUrl = "http://localhost:8080"
		noteId      = "2GT593V41"
		paragraphId = "paragraph_1640616010551_350554117"
	)

	client := NewZeppelinClient()
	result, err := client.waitUntilRunning(zeppelinUrl, noteId, paragraphId)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("jobId %s ===== stataus %s ====  data %s", result.JobId, result.Status, result.Data)
}
