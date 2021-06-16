package executor

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type HttpClient struct {
	ZeppelinServer string
	Client         *http.Client
}

const (
	ParagraphUnknown = "UNKNOWN"
	ParagraphFinish  = "FINISHED"
	ParagraphRunning = "RUNNING"
	ParagraphReady   = "READY"
	ParagraphError   = "ERROR"
	ParagraphPending = "PENDING"
	ParagraphAbort   = "ABORT"
)

func NewHttpClient(serverAddr string) HttpClient {
	return HttpClient{ZeppelinServer: "http://" + serverAddr, Client: &http.Client{Timeout: time.Second * 60}}
}

func doRequest(client *http.Client, method string, status int, api string, body string, retJson bool) (repJson map[string]string, repString string, err error) {
	var (
		req     *http.Request
		rep     *http.Response
		reqBody io.Reader
	)

	if body == "" {
		reqBody = nil
	} else {
		reqBody = strings.NewReader(body)
	}

	req, err = http.NewRequest(method, api, reqBody)
	if err != nil {
		return
	}

	rep, err = client.Do(req)
	if err != nil {
		rep.Body.Close()
		return
	}

	repBody, _ := ioutil.ReadAll(rep.Body)
	rep.Body.Close()

	repString = string(repBody)
	if retJson == true {
		err = json.Unmarshal(repBody, &repJson)
		if err != nil {
			rep.Body.Close()
			return
		}
	}

	if rep.StatusCode != status {
		err = fmt.Errorf("%s request failed, http status code %d, message %s", api, rep.StatusCode, repString)
		rep.Body.Close()
		return
	}

	return
}

func (ex *HttpClient) CreateNote(ID string) (noteID string, err error) {
	var repJson map[string]string

	repJson, _, err = doRequest(ex.Client, http.MethodPost, http.StatusOK, ex.ZeppelinServer+"/api/notebook", fmt.Sprintf("{\"name\": \"%s\"}", ID), true)
	if err != nil {
		return
	}
	noteID = repJson["body"]

	return noteID, nil
}

func (ex *HttpClient) DeleteNote(ID string) (err error) {
	_, _, err = doRequest(ex.Client, http.MethodDelete, http.StatusOK, ex.ZeppelinServer+"/api/notebook/"+ID, "", false)
	return
}

func (ex *HttpClient) StopAllParagraphs(noteID string) (err error) {
	_, _, err = doRequest(ex.Client, http.MethodDelete, http.StatusOK, ex.ZeppelinServer+"/api/notebook/job/"+noteID, "", false)
	return
}

func (ex *HttpClient) CreateParagraph(noteID string, index int32, name string, text string) (paragraphID string, err error) {
	var repJson map[string]string

	repJson, _, err = doRequest(ex.Client, http.MethodPost, http.StatusOK, ex.ZeppelinServer+"/api/notebook/"+noteID+"/paragraph", fmt.Sprintf("{\"title\": \"%s\", \"text\": \"%s\", \"index\": %d}", name, text, index), true)
	if err != nil {
		return
	}
	paragraphID = repJson["body"]

	return paragraphID, nil
}

func (ex *HttpClient) RunParagraphSync(noteID string, paragraphID string) (err error) {
	_, _, err = doRequest(ex.Client, http.MethodPost, http.StatusOK, ex.ZeppelinServer+"/api/notebook/run/"+noteID+"/"+paragraphID, "", false)
	return
}

func (ex *HttpClient) RunParagraphAsync(noteID string, paragraphID string) (err error) {
	_, _, err = doRequest(ex.Client, http.MethodPost, http.StatusOK, ex.ZeppelinServer+"/api/notebook/job/"+noteID+"/"+paragraphID, "", false)
	return
}

func (ex *HttpClient) GetParagraphStatus(noteID string, paragraphID string) (status string, err error) {
	var repString string
	var repJsonLevel1 map[string]json.RawMessage
	var repJsonLevel2 map[string]string

	_, repString, err = doRequest(ex.Client, http.MethodGet, http.StatusOK, ex.ZeppelinServer+"/api/notebook/job/"+noteID+"/"+paragraphID, "", false)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(repString), &repJsonLevel1)
	if err != nil {
		return
	}
	err = json.Unmarshal(repJsonLevel1["body"], &repJsonLevel2)
	if err != nil {
		return
	}
	status = repJsonLevel2["status"]

	return
}

func (ex *HttpClient) GetParagraphResultOutput(noteID string, paragraphID string) (msg string, err error) {
	var repString string
	var repJsonLevel1 map[string]json.RawMessage
	var repJsonLevel2 map[string]json.RawMessage
	var repJsonLevel3 map[string]json.RawMessage

	_, repString, err = doRequest(ex.Client, http.MethodGet, http.StatusOK, ex.ZeppelinServer+"/api/notebook/"+noteID+"/paragraph/"+paragraphID, "", false)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(repString), &repJsonLevel1)
	if err != nil {
		return
	}
	err = json.Unmarshal(repJsonLevel1["body"], &repJsonLevel2)
	if err != nil {
		return
	}
	err = json.Unmarshal(repJsonLevel2["results"], &repJsonLevel3)
	if err != nil {
		return
	}

	msg = string(repJsonLevel3["msg"])

	return
}
