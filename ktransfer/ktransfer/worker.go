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
	"container/list"
	"errors"
	"fmt"
	"github.com/dzch/go-utils/logger"
	"time"
)

type TransWorker struct {
	moduleName        string
	workerId          uint32
	backendServers    []string
	protocolConfig    map[interface{}]interface{}
	protocolName      string
	protocol          Protocol
	fatalErrorChan    chan error
	inWork            bool
	transDataChan     chan *TransData
	ackDataChan       chan *AckData
	transQueue        *list.List
	lastServerId      int
	serverNum         int
	maxRetryTimes     int // -1, 无限重试
	failRetryInterval time.Duration
}

func (worker *TransWorker) init() (err error) {
	logger.Debug("init trans worker for module [%s]: %d", worker.moduleName, worker.workerId)
	err = worker.initProtocol()
	if err != nil {
		return
	}
	worker.transQueue = list.New()
	worker.transDataChan = make(chan *TransData)
	worker.inWork = false
	worker.lastServerId = -1
	worker.serverNum = len(worker.backendServers)
	return nil
}

func (worker *TransWorker) run() {
	logger.Debug("worker %d start for module [%s]", worker.workerId, worker.moduleName)
	for transData := range worker.transDataChan {
		logger.Debug("worker begin: transid=%d, topic=%s, method=%s", transData.transid, transData.topic, transData.method)
		/* will block in worker.trans untile succeed in trans or reach retry limit */
		worker.trans(transData)
	}
}

func (worker *TransWorker) initProtocol() (err error) {
	name, ok := worker.protocolConfig["name"]
	if !ok {
		err = errors.New(fmt.Sprintf("protocol.name not found in module conf file, module [%s]", worker.moduleName))
		logger.Warning("%s", err.Error())
		return
	}
	worker.protocolName = name.(string)
	switch worker.protocolName {
	case "http":
		err = worker.initHttpProtocol()
	default:
		err = errors.New(fmt.Sprintf("unknown protocol: %s", worker.protocolName))
	}
	if err != nil {
		logger.Warning("fail to init protocol for module [%s]: %s", worker.moduleName, err.Error())
		return
	}
	return nil
}

func (worker *TransWorker) initHttpProtocol() (err error) {
	worker.protocol = &HttpProtocol{
		protocolName: worker.protocolName,
		moduleName:   worker.moduleName,
		config:       worker.protocolConfig,
	}
	return worker.protocol.init()
}

func (worker *TransWorker) trans(transData *TransData) {
	var err error
	retry := 0
	for {
		serverId := (worker.lastServerId + 1) % (worker.serverNum)
		worker.lastServerId = serverId
		startTime := time.Now()
		err = worker.protocol.transData(worker.backendServers[serverId], transData)
		timeUsedMs := int(time.Now().Sub(startTime) / time.Millisecond)
		if err == nil {
			logger.Notice("module [%s]: worker[%d] success transData: server=%s, topic=%s, partition=%d, transid=%d, method=%s, proctime=%dms", worker.moduleName, worker.workerId, worker.backendServers[serverId], transData.topic, transData.partition, transData.transid, transData.method, timeUsedMs)
			ackData := &AckData{
				transid:  transData.transid,
				workerId: worker.workerId,
			}
			worker.ackDataChan <- ackData
			logger.Debug("add ackData: transid=%d", ackData.transid)
			return
		}
		logger.Notice("module [%s]: worker[%d] fail transData: server=%s, topic=%s, partition=%d, transid=%d, method=%s, proctime=%dms", worker.moduleName, worker.workerId, worker.backendServers[serverId], transData.topic, transData.partition, transData.transid, transData.method, timeUsedMs)
		/* check need retry */
		if worker.maxRetryTimes == -1 || retry < worker.maxRetryTimes {
			retry++
			time.Sleep(worker.failRetryInterval)
			continue
		}
		/* reatch retry limit */
		ackData := &AckData{
			transid:  transData.transid,
			workerId: worker.workerId,
		}
		worker.ackDataChan <- ackData
		logger.Debug("add ackData: transid=%d", ackData.transid)
		return
	}
}

/* Notice: this func shall ONLY be called in transdi routine */
func (worker *TransWorker) addTrans(transData *TransData) (err error) {
	worker.transQueue.PushBack(transData)
	return worker.workIfNeed()
}

/* Notice: this func shall ONLY be called in transdi routine */
func (worker *TransWorker) workIfNeed() (err error) {
	if !worker.inWork {
		e := worker.transQueue.Front()
		if e == nil {
			return nil
		}
		transData := e.Value.(*TransData)
		logger.Debug("add to worker chan: transid=%d, topic=%s, method=%s", transData.transid, transData.topic, transData.method)
		worker.transDataChan <- transData
		worker.inWork = true
		worker.transQueue.Remove(e)
	}
	return nil
}
