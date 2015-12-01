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
package kproxy

import (
		"github.com/dzch/go-utils/logger"
		"errors"
        "fmt"
		"net/http"
		"os"
		"time"
	   )

type KProxy struct {
	confFile string
	config *Config
	server *http.Server
	producer *Producer
	fatalErrorChan chan *error
}

func NewKProxy(confFile string) (*KProxy, error) {
    kp := &KProxy {
            confFile: confFile,
		}
	err := kp.init()
	if err != nil {
		return nil, err
	}
	return kp, nil
}

func (kp *KProxy) init() error {
	var err error
	err = kp.initConfig()
	if err != nil {
		return err
	}
	err = kp.initLog()
	if err != nil {
		return err
	}
	err = kp.initChans()
	if err != nil {
		return err
	}
    err = kp.initHttpServer()
	if err != nil {
	    return err
	}
	err = kp.initProducer()
	if err != nil {
		return err
	}
	return nil
}

func (kp *KProxy) initLog() error {
    return logger.Init(kp.config.logDir, "kproxy", logger.LogLevel(kp.config.logLevel))
}

func (kp *KProxy) initConfig() error {
	var err error
    kp.config, err = newConfig(kp.confFile)
	if err != nil {
		return err
	}
	return nil
}

func (kp *KProxy) initChans() error {
	kp.fatalErrorChan = make(chan *error, 1)
	return nil
}

func (kp *KProxy) initHttpServer() error {
    mux := http.NewServeMux()
	h, err := newCmHandler(kp)
	if err != nil {
		return err
	}
	mux.Handle("/", h)
    kp.server = &http.Server {
        Addr: fmt.Sprintf(":%d", kp.config.httpServerListenPort),
		Handler: mux,
		ReadTimeout: kp.config.httpServerReadTimeout,
		WriteTimeout: kp.config.httpServerWriteTimeout,
	}
	return nil
}

func (kp *KProxy) initProducer() error {
	var err  error
	kp.producer, err = newProducer(kp)
	if err != nil {
	    return err
	}
	return nil
}

func (kp *KProxy) Run() {
	go kp.producer.run()
	go kp.httpServerRun()
	err := <-kp.fatalErrorChan
	logger.Fatal("%s", (*err).Error())
	time.Sleep(1)
	os.Exit(1)
}

func (kp *KProxy) httpServerRun() {
    err := kp.server.ListenAndServe()
	if err != nil {
		logger.Fatal("fail to start http server: %s", err.Error())
	    kp.fatalErrorChan <-&err
		return
	}
	err = errors.New("http server done")
	kp.fatalErrorChan <-&err
}
