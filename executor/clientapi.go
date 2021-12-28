package executor

import (
	"fmt"
	"github.com/DataWorkbench/common/qerror"
	"github.com/ddliu/go-httpclient"
	"github.com/valyala/fastjson"
	"strings"
	"time"
)

const (
	RUNNING  = "RUNNING"
	PENDING  = "PENDING"
	FINISHED = "FINISHED"
	ABORT    = "ABORT"
	ERROR    = "ERROR"
	UNKNOWN  = "UNKNOWN"
	READY    = "READY"
)

func NewZeppelinClient() *ZeppelinClient {
	return &ZeppelinClient{httpclient.NewHttpClient()}
}

type ZeppelinClient struct {
	*httpclient.HttpClient
}

func (client *ZeppelinClient) submit(zeppelinUrl string, nodeId string, paragraphId string) error {
	runUrl := fmt.Sprintf("%s/api/notebook/job/%s/%s", zeppelinUrl, nodeId, paragraphId)
	res, err := client.Post(runUrl, "")
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		//TODO define error
		return qerror.Internal
	}
	return nil
}

func (client *ZeppelinClient) stop(zeppelinUrl string, nodeId string, paragraphId string) error {
	stopUrl := fmt.Sprintf("%s/api/notebook/job/%s/%s", zeppelinUrl, nodeId, paragraphId)
	res, err := client.Delete(stopUrl)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return qerror.Internal
	}
	return nil
}

type QueryResponse struct {
	JobId  string
	Data   string
	Status string
}

func (client *ZeppelinClient) queryResult(zeppelinUrl string, noteId string, paragraphId string) (queryResponse *QueryResponse, err error) {
	queryUrl := fmt.Sprintf("%s/api/notebook/%s/paragraph/%s", zeppelinUrl, noteId, paragraphId)
	var (
		parser   fastjson.Parser
		parseVal *fastjson.Value
	)

	res, err := client.Get(queryUrl)
	if err != nil {
		return
	}
	if res.StatusCode != 200 {
		err = qerror.Internal
		return
	}
	var b []byte
	if b, err = res.ReadAll(); err != nil {
		return
	}
	if parseVal, err = parser.Parse(string(b)); err != nil {
		return
	}
	statusBytes := parseVal.Get("body").GetStringBytes("status")
	fmt.Println(string(statusBytes))
	switch string(statusBytes) {
	case RUNNING:
		url := parseVal.Get("body").Get("runtimeInfos").Get("jobUrl")
		if url != nil {
			values := url.GetArray("values")
			if len(values) > 0 {
				jobUrl := string(values[0].GetStringBytes("jobUrl"))
				jobId := jobUrl[strings.LastIndex(jobUrl, "/")+1:]
				if len(jobId) != 32 {
					err = qerror.Internal
					return
				}
				status := string(statusBytes)
				queryResponse = &QueryResponse{
					JobId:  jobId,
					Data:   "",
					Status: status,
				}
				return
			}
		}
	case FINISHED, ERROR, ABORT:
		msgArr := parseVal.Get("body").Get("results").GetArray("msg")
		var data string
		for _, msg := range msgArr {
			msgBytes := msg.GetStringBytes("type")
			if msgBytes != nil && strings.EqualFold(string(msgBytes), "TEXT") {
				dataBytes := msg.GetStringBytes("data")
				if dataBytes != nil {
					data = string(dataBytes)
				}
			}
		}
		status := string(statusBytes)
		queryResponse = &QueryResponse{
			JobId:  "",
			Data:   data,
			Status: status,
		}
		return
	case UNKNOWN:
		// TODO how unknown status return,wait until call stop api ?
		status := string(statusBytes)
		data := parseVal.String()
		queryResponse = &QueryResponse{
			JobId:  "",
			Data:   data,
			Status: status,
		}
	case PENDING:
		return
	}
	return
}

func (client *ZeppelinClient) waitUntilRunning(zeppelinUrl string, noteId string, paragraphId string) (queryResponse *QueryResponse, err error) {
	var count = 0

	if err = client.submit(zeppelinUrl, noteId, paragraphId); err != nil {
		return
	}

	for {
		if queryResponse, err = client.queryResult(zeppelinUrl, noteId, paragraphId); err != nil {
			return
		}
		if queryResponse != nil {
			return
		}
		if count >= 20 {
			if err = client.stop(zeppelinUrl, noteId, paragraphId); err != nil {
				return
			}
		}
		time.Sleep(time.Second * 2)
		count += 2
	}
}
