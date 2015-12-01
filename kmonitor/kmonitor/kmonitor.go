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
		"os"
		"net/http"
		"fmt"
	   )

type KMonitor struct {
	confFile string
	config *Config
	clusters map[string]*Cluster
	fatalErrorChan chan error
	server *http.Server
}

func NewKMonitor(confFile string) (*KMonitor, error) {
    km := &KMonitor{
		confFile: confFile,
	}
    err := km.init()
	if err != nil {
		return nil, err
	}
	return km, nil
}

func (km *KMonitor) Run() {
	for _, c := range km.clusters {
		go c.run()
	}
	go km.processFatalError()
	err := km.server.ListenAndServe()
	if err != nil {
		logger.Fatal(err.Error())
	}
}

func (km *KMonitor) init() error {
	err := km.initConfig()
	if err != nil {
		return err
	}
	err = km.initLog()
	if err != nil {
		return err
	}
	err = km.initChans()
	if err != nil {
		return err
	}
	err = km.initCluster()
	if err != nil {
		return err
	}
	err = km.initServer()
	if err != nil {
		return err
	}
	return nil
}

func (km *KMonitor) initConfig() error {
	var err error
	km.config, err = newConfig(km.confFile)
	if err != nil {
		return err
	}
	return nil
}

func (km *KMonitor) initLog() error {
    return logger.Init(km.config.bc.logDir, "kmonitor", logger.LogLevel(km.config.bc.logLevel))
}

func (km *KMonitor) initChans() error {
	km.fatalErrorChan = make(chan error, 1)
	return nil
}

func (km *KMonitor) initCluster() error {
	km.clusters = make(map[string]*Cluster)
	for name, _ := range km.config.cc {
	    cluster, err := newCluster(km, name)
	    if err != nil {
		    return err
	    }
		km.clusters[name] = cluster
	}
	return nil
}

func (km *KMonitor) initServer() error {
    mux := http.NewServeMux()
	handler, err := newDelayHandler(km)
	if err != nil {
		return err
	} else {
	    mux.Handle("/delay", handler)
	}
    km.server = &http.Server {
        Addr: fmt.Sprintf(":%d", km.config.hsc.port),
		Handler: mux,
		ReadTimeout: km.config.hsc.readTimeout,
		WriteTimeout: km.config.hsc.writeTimeout,
	}
	return nil
}

func (km *KMonitor) processFatalError() {
    err := <-km.fatalErrorChan
	logger.Fatal("%s", err.Error())
	os.Exit(255)
}
