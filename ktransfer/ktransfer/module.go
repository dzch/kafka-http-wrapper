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
	"fmt"
	"github.com/dzch/go-utils/logger"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

type Module struct {
	name                   string
	zkHosts                []string
	zkChroot               string
	zkTimeout              time.Duration
	zkFailRetryInterval    time.Duration
	moduleConfDir          string
	moduleConfig           map[interface{}]interface{}
	topic                  string
	methodEnabled          map[string]bool
	protocolConfig         map[interface{}]interface{}
	workerNum              uint32
	maxRetryTimes          int // -1, 无限重试
	failRetryInterval      time.Duration
	windowSize             int
	backendServers         []string
	transDi                *TransDi
	fatalErrorChan         chan error
	expectedProcessingTime time.Duration
	waitingQueueSize       int
	zkOffsetUpdateInterval time.Duration
	serializeByKey         bool
}

func (m *Module) init() (err error) {
	err = m.initConfig()
	if err != nil {
		return
	}
	err = m.initTransDi()
	if err != nil {
		return
	}
	return nil
}

func (m *Module) run() {
	logger.Notice("module [%s] start", m.name)
	m.transDi.run()
	// TODO: delete it later
	m.fatalErrorChan <- errors.New("no error")
}

func (m *Module) initConfig() (err error) {
	content, err := ioutil.ReadFile(fmt.Sprintf("%s/%s.yaml", m.moduleConfDir, m.name))
	if err != nil {
		logger.Warning("fail to read module config: %s", err.Error())
		return
	}
	mc := make(map[interface{}]interface{})
	err = yaml.Unmarshal(content, &mc)
	if err != nil {
		return
	}
	m.moduleConfig = mc

	/* topic conf */
	topic, ok := mc["topic"]
	if !ok {
		err = errors.New(fmt.Sprintf("topic not found in module conf file, module [%s]", m.name))
		logger.Warning("%s", err.Error())
		return
	}
	m.topic = topic.(string)

	/* method conf */
	m.methodEnabled = make(map[string]bool)
	methods, ok := mc["methods"]
	if ok {
		for _, method := range methods.([]interface{}) {
			m.methodEnabled[method.(string)] = true
		}
	}

	/* protocol conf */
	protocol, ok := mc["protocol"]
	if !ok {
		err = errors.New(fmt.Sprintf("protocol not found in module conf file, module [%s]", m.name))
		logger.Warning("%s", err.Error())
		return
	}
	m.protocolConfig = protocol.(map[interface{}]interface{})

	/* worker conf */
	workerNum, ok := mc["worker_num"]
	if !ok {
		err = errors.New(fmt.Sprintf("worker_num not found in module conf file, module [%s]", m.name))
		logger.Warning("%s", err.Error())
		return
	}
	m.workerNum = uint32(workerNum.(int))
	maxRetryTimes, ok := mc["max_retry_times"]
	if !ok {
		err = errors.New(fmt.Sprintf("max_retry_times not found in module conf file, module [%s]", m.name))
		logger.Warning("%s", err.Error())
		return
	}
	m.maxRetryTimes = maxRetryTimes.(int)
	failRetryInterval, ok := mc["fail_retry_interval_ms"]
	if !ok {
		err = errors.New(fmt.Sprintf("fail_retry_interval_ms not in module conf file, module [%s]", m.name))
		logger.Warning("%s", err.Error())
		return
	}
	m.failRetryInterval = time.Duration(failRetryInterval.(int)) * time.Millisecond
	sbykey, ok := mc["serialize_by_key"]
	if ok {
		m.serializeByKey = sbykey.(bool)
	} else {
		m.serializeByKey = true
	}

	/* window size */
	windowSize, ok := mc["window_size"]
	if !ok {
		err = errors.New(fmt.Sprintf("window_size not found in module conf file, module [%s]", m.name))
		logger.Warning("%s", err.Error())
		return
	}
	m.windowSize = windowSize.(int)

	/* backend servers */
	backendServers, ok := mc["backend_servers"]
	if !ok {
		err = errors.New(fmt.Sprintf("backend_servers not found in module conf file, module [%s]", m.name))
		logger.Warning("%s", err.Error())
		return
	}
	for _, server := range backendServers.([]interface{}) {
		m.backendServers = append(m.backendServers, server.(string))
	}

	/* consumer config */
	ti, ok := mc["expected_processing_time_ms"]
	if ok {
		m.expectedProcessingTime = time.Duration(ti.(int)) * time.Millisecond
	}
	ti, ok = mc["zk_offset_update_interval_sec"]
	if ok {
		m.zkOffsetUpdateInterval = time.Duration(ti.(int)) * time.Second
	}
	qs, ok := mc["waiting_queue_size"]
	if ok {
		m.waitingQueueSize = qs.(int)
	}

	return nil
}

func (m *Module) initTransDi() (err error) {
	m.transDi = &TransDi{
		moduleName:             m.name,
		zkHosts:                m.zkHosts,
		zkChroot:               m.zkChroot,
		zkTimeout:              m.zkTimeout,
		zkFailRetryInterval:    m.zkFailRetryInterval,
		backendServers:         m.backendServers,
		protocolConfig:         m.protocolConfig,
		windowSize:             m.windowSize,
		workerNum:              m.workerNum,
		fatalErrorChan:         m.fatalErrorChan,
		topic:                  m.topic,
		methodEnabled:          m.methodEnabled,
		maxRetryTimes:          m.maxRetryTimes,
		failRetryInterval:      m.failRetryInterval,
		expectedProcessingTime: m.expectedProcessingTime,
		waitingQueueSize:       m.waitingQueueSize,
		zkOffsetUpdateInterval: m.zkOffsetUpdateInterval,
		serializeByKey:         m.serializeByKey,
	}
	return m.transDi.init()
}
