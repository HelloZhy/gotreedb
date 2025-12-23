package controller

import (
	"encoding/json"
	"errors"
	"net/http"
)

var errNotImpletement = errors.New("interface not impletement")

func writeJsonResp(w http.ResponseWriter, resp any) error {
	w.Header().Add("Content-Type", "application/json")

	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	w.Write(b)

	return nil
}

type ErrResp struct {
	Error string `json:"error"`
}

func writeErrJsonResp(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusNotFound)

	writeJsonResp(w, ErrResp{Error: err.Error()})
}
