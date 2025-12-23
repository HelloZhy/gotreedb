package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/HelloZhy/gotreedb/internal/controller"
	"github.com/HelloZhy/gotreedb/internal/resource"
	"github.com/google/uuid"
)

var server *httptest.Server

func TestMain(m *testing.M) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	_resource := resource.Resource{}
	_controller := controller.Controller{}

	mux := &http.ServeMux{}

	startup(&_resource, &_controller, mux)

	server = httptest.NewServer(mux)
	defer server.Close()

	os.Exit(m.Run())
}

func printErrJsonRespWithTestFailed(t *testing.T, resp *http.Response) {
	b, _ := io.ReadAll(resp.Body)

	var jsonResp controller.ErrResp
	if err := json.Unmarshal(b, &jsonResp); err != nil {
		t.Fatalf("error response decode error: %+v, json bytes: %s", err, string(b))
	}

	t.Fatalf("error response: %+v", jsonResp)
}

func TestSmoke(t *testing.T) {
	req1 := controller.PostRequestReq{
		ReadBatch: &controller.ReadBatch{
			Ops: []controller.ReadOp{
				{
					Entry: "/",
					Keys:  []string{"key1", "key2", "key3"},
				},
				{
					Entry: "/node1",
					Keys:  []string{"key11"},
				},
				{
					Entry: "/node2",
					Keys:  []string{},
				},
			},
		},
	}

	_uuid1 := verifyPostRequest(t, req1)

	req2 := controller.PostRequestReq{
		WriteBatch: &controller.WriteBatch{
			Ops: []controller.WriteOp{
				{
					Entry: "/",
					CreateAction: &controller.CreateAction{
						KVPairs: []controller.KVPair{
							{Key: "key5", Value: "value5"},
							{Key: "key6", Value: "value6"},
						},
						Override: true,
					},
				},
				{
					Entry: "/node3",
					UpdateAction: &controller.UpdateAction{
						KVPairs: []controller.KVPair{
							{Key: "key35", Value: "value35"},
							{Key: "key36", Value: "value36"},
						},
					},
				},
				{
					Entry: "/node4",
					DeleteAction: &controller.DeleteAction{
						Keys: []string{"key41", "key42"},
					},
				},
			},
		},
	}
	_uuid2 := verifyPostRequest(t, req2)

	verifyGetRequests(t)
	verifyGetRequest(t, _uuid1)
	verifyGetRequest(t, _uuid2)
}

func verifyPostRequest(t *testing.T, req controller.PostRequestReq) (_uuid uuid.UUID) {
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(fmt.Sprintf("%s/api/requests", server.URL), "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK {
		printErrJsonRespWithTestFailed(t, resp)
	}

	b, _ = io.ReadAll(resp.Body)
	jsonResp := controller.PostRequestResp{}
	err = json.Unmarshal(b, &jsonResp)
	if err != nil {
		t.Fatal(err)
	}

	_uuid = jsonResp.UUID

	return
}

func verifyGetRequests(t *testing.T) {
	resp, err := http.Get(fmt.Sprintf("%s/api/requests", server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		printErrJsonRespWithTestFailed(t, resp)
	}
	var jsonResp controller.GetRequestsResp

	b, _ := io.ReadAll(resp.Body)
	json.Unmarshal(b, &jsonResp)

	t.Logf("%+v", jsonResp)
}

func verifyGetRequest(t *testing.T, _uuid uuid.UUID) {
	resp, err := http.Get(fmt.Sprintf("%s/api/requests/%s", server.URL, _uuid.String()))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		printErrJsonRespWithTestFailed(t, resp)
	}
	var jsonResp controller.GetRequestResp

	b, _ := io.ReadAll(resp.Body)
	json.Unmarshal(b, &jsonResp)

	t.Logf("%+v", jsonResp)
}
