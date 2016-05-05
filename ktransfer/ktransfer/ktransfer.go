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
package ktransfer

import (
	"errors"
	"github.com/dzch/go-utils/logger"
	"gopkg.in/Shopify/sarama.v1"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

type KTransfer struct {
	confFile            string
	logDir              string
	logLevel            int
	zkHosts             []string
	zkChroot            string
	zkTimeout           time.Duration
	zkFailRetryInterval time.Duration
	moduleEnabled       []string
	moduleConfDir       string
	modules             map[string]*Module
	fatalErrorChan      chan error
}

func NewKTransfer(confFile string) (*KTransfer, error) {
	transfer := &KTransfer{
		confFile: confFile,
	}
	return transfer, transfer.init()
}

func (transfer *KTransfer) init() (err error) {
	err = transfer.initConfig()
	if err != nil {
		return
	}
	err = transfer.initLog()
	if err != nil {
		return
	}
	err = transfer.initChans()
	if err != nil {
		return
	}
	err = transfer.initModules()
	if err != nil {
		return
	}
	return nil
}

func (transfer *KTransfer) initConfig() (err error) {
	content, err := ioutil.ReadFile(transfer.confFile)
	if err != nil {
		return
	}
	m := make(map[interface{}]interface{})
	err = yaml.Unmarshal(content, &m)
	if err != nil {
		return
	}

	/* log conf */
	logDir, ok := m["log_dir"]
	if !ok {
		return errors.New("log_dir not found in conf file")
	}
	transfer.logDir = logDir.(string)
	logLevel, ok := m["log_level"]
	if !ok {
		return errors.New("log_level not found in conf file")
	}
	transfer.logLevel = logLevel.(int)

	/* zk conf */
	zkhosts, ok := m["zk_hosts"]
	if !ok {
		return errors.New("zk_hosts not found in conf file")
	}
	zkHosts := zkhosts.([]interface{})
	if len(zkHosts) <= 0 {
		return errors.New("num of zkHosts is zero")
	}
	for _, zkhost := range zkHosts {
		transfer.zkHosts = append(transfer.zkHosts, zkhost.(string))
	}
	zkChroot, ok := m["zk_chroot"]
	if !ok {
		transfer.zkChroot = ""
	} else {
		transfer.zkChroot = zkChroot.(string)
	}
	zkTimeout, ok := m["zk_timeout_ms"]
	if !ok {
		transfer.zkTimeout = 30 * time.Second
	} else {
		transfer.zkTimeout = time.Duration(zkTimeout.(int)) * time.Millisecond
	}
	zkTi, ok := m["zk_fail_retry_interval_ms"]
	if !ok {
		transfer.zkFailRetryInterval = 30 * time.Second
	} else {
		transfer.zkFailRetryInterval = time.Duration(zkTi.(int)) * time.Millisecond
	}

	/* module conf */
	moduleConfDir, ok := m["module_conf_dir"]
	if !ok {
		return errors.New("module_conf_dir not found in conf file")
	}
	transfer.moduleConfDir = moduleConfDir.(string)
	modules, ok := m["module_enabled"]
	if !ok {
		return errors.New("module_enabled not found in conf file")
	}
	moduleConfs := modules.([]interface{})
	if len(moduleConfs) <= 0 {
		return errors.New("num of module_enabled is zero")
	}
	for _, module := range moduleConfs {
		transfer.moduleEnabled = append(transfer.moduleEnabled, module.(string))
	}

	return nil
}

func (transfer *KTransfer) initLog() (err error) {
	err = logger.Init(transfer.logDir, "ktransfer", logger.LogLevel(transfer.logLevel))
	if err != nil {
		return
	}
	sarama.Logger = newSaramaLogger()
	return
}

func (transfer *KTransfer) initModules() (err error) {
	transfer.modules = make(map[string]*Module)
	for _, moduleName := range transfer.moduleEnabled {
		module := &Module{
			name:                   moduleName,
			zkHosts:                transfer.zkHosts,
			zkChroot:               transfer.zkChroot,
			zkTimeout:              transfer.zkTimeout,
			zkFailRetryInterval:    transfer.zkFailRetryInterval,
			moduleConfDir:          transfer.moduleConfDir,
			fatalErrorChan:         transfer.fatalErrorChan,
			expectedProcessingTime: 100 * time.Millisecond,
			waitingQueueSize:       256,
			zkOffsetUpdateInterval: 10 * time.Second,
		}
		err = module.init()
		if err != nil {
			return
		}
		transfer.modules[moduleName] = module
		logger.Notice("succeeded in initing module [%s]", moduleName)
	}
	logger.Notice("all modules enabled have been inited")
	return nil
}

func (transfer *KTransfer) initChans() (err error) {
	transfer.fatalErrorChan = make(chan error)
	return nil
}

func (transfer *KTransfer) Run() {
	logger.Notice("ktransfer start")
	for _, module := range transfer.modules {
		go module.run()
	}
	for {
		select {
		case err := <-transfer.fatalErrorChan:
			logger.Fatal("%s", err.Error())
			return
		}
	}
	return
}
