/*
    The MIT License (MIT)
    
	Copyright (c) 2015 myhug.cn and zhouwench (zhouwench@gmail.com)
    
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
package kmonitor

import (
		"github.com/dzch/go-utils/logger"
		"net/http"
		"encoding/json"
		"bytes"
		"fmt"
	   )

type DelayHandler struct {
	km *KMonitor
}

func newDelayHandler(km *KMonitor) (*DelayHandler, error) {
    h := &DelayHandler {
		km: km,
	}
	err := h.init()
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *DelayHandler) init() error {
	return nil
}

func (h *DelayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    values := r.URL.Query()
	format := values.Get("format")
	if len(format) == 0 {
		format = "json"
	}
	clusterName := values.Get("cluster")
	if len(clusterName) == 0 {
		h.getClusterDelayStatus(h.km.clusters, format, w, r)
	} else {
		c, ok := h.km.clusters[clusterName]
		if !ok {
			logger.Warning("cluster not exisits: cluster=%s", clusterName)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		h.getClusterDelayStatus(map[string]*Cluster{clusterName: c}, format, w, r)
	}
}

func (h *DelayHandler) getClusterDelayStatus(clusters map[string]*Cluster, format string, w http.ResponseWriter, r *http.Request) {
    res := make(map[string]TransDelay)
	for cname, c := range clusters {
        td := c.getTransDelay()
		res[cname] = td
	}
	var resData []byte
	var err error
	switch format {
		case "json":
	        resData, err = json.Marshal(res)
	        if err != nil {
		        logger.Warning("fail to json.Marshal: %s", err.Error())
		        w.WriteHeader(http.StatusInternalServerError)
		        return
			}
		case "zabbix":
			resData, err = h.zabbixFormat(res)
			if err != nil {
		        logger.Warning("fail to zabbixFormat: %s", err.Error())
		        w.WriteHeader(http.StatusInternalServerError)
		        return
			}
		default:
		   logger.Warning("invalid format")
		   w.WriteHeader(http.StatusBadRequest)
		   return
	}
	w.Write(resData)
}

func (h *DelayHandler) zabbixFormat(res map[string]TransDelay) ([]byte, error) {
    var buf bytes.Buffer
	for cluster, td := range res {
		for cg, pd := range td {
			for partition, delay := range pd {
				fmt.Fprintf(&buf, "- %s.%s.%s.delay %d\n", cluster, cg, partition, delay)
			}
		}
	}
	return buf.Bytes(), nil
}
