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
	"bytes"
	"errors"
	"fmt"
	"github.com/dzch/go-utils/logger"
	"github.com/dzch/kafka/consumergroup"
	"github.com/tinylib/msgp/msgp"
	"gopkg.in/Shopify/sarama.v1"
	"hash"
	"hash/fnv"
	"math/rand"
	"time"
)

const (
	WAIT_ACK_NONBLOCK = iota
	WAIT_ACK_BLOCK
	CHECK_NEED_WAIT_ACK
	WAIT_ACK_AND_DI
)

var (
	gDefaultMethod = "*"
)

type TransData struct {
	topic     string
	partition int32
	method    string
	key       string
	transid   int64
	data      []byte
}

type AckData struct {
	workerId uint32
	transid  int64
}

type TransDi struct {
	moduleName             string
	zkHosts                []string
	zkChroot               string
	zkTimeout              time.Duration
	zkFailRetryInterval    time.Duration
	ackedMinTransid        int64
	curReadTransid         int64 // 已经读取的最大transid
	curDiTransid           int64
	protocolConfig         map[interface{}]interface{}
	backendServers         []string
	windowSize             int
	workerNum              uint32
	transWorkers           []*TransWorker
	fatalErrorChan         chan error
	state                  int
	ackDataChan            chan *AckData
	transWindow            *TransWindow
	topic                  string
	methodEnabled          map[string]bool
	maxRetryTimes          int // -1, 无限重试
	failRetryInterval      time.Duration
	cg                     *consumergroup.ConsumerGroup
	dispatchHasher         hash.Hash32
	expectedProcessingTime time.Duration
	waitingQueueSize       int
	zkOffsetUpdateInterval time.Duration
	serializeByKey         bool
}

func (td *TransDi) init() (err error) {
	err = td.initChans()
	if err != nil {
		return
	}
	err = td.initTransWindow()
	if err != nil {
		return
	}
	err = td.initTransWorkers()
	if err != nil {
		return
	}
	err = td.initConsumer()
	if err != nil {
		return
	}
	err = td.initDispatchHash()
	if err != nil {
		return
	}
	return nil
}

func (td *TransDi) run() {
	logger.Debug("transdi start for module [%s]", td.moduleName)
	for _, worker := range td.transWorkers {
		go worker.run()
	}
	td.state = WAIT_ACK_NONBLOCK
	for {
		switch td.state {
		case WAIT_ACK_NONBLOCK:
			td.waitAckNonBlock()
			continue
		case CHECK_NEED_WAIT_ACK:
			td.checkNeedWaitAck()
			continue
		case WAIT_ACK_BLOCK:
			td.waitAckBlock()
			continue
		case WAIT_ACK_AND_DI:
			td.waitAckAndDi()
			continue
		default:
			td.fatalErrorChan <- errors.New(fmt.Sprintf("invalid transdi run state: %d", td.state))
			break
		}
	}
}

func (td *TransDi) initChans() (err error) {
	td.ackDataChan = make(chan *AckData)
	return nil
}

func (td *TransDi) initTransWindow() (err error) {
	td.transWindow = &TransWindow{
		windowSize: td.windowSize,
	}
	return td.transWindow.init()
}

func (td *TransDi) initConsumer() (err error) {
	config := consumergroup.NewConfig()
	config.Metadata.RefreshFrequency = 60 * time.Second
	config.Consumer.MaxProcessingTime = td.expectedProcessingTime
	config.ChannelBufferSize = td.waitingQueueSize
	config.Zookeeper.Chroot = td.zkChroot
	config.Zookeeper.Timeout = td.zkTimeout
	config.Zookeeper.FailRetryInterval = td.zkFailRetryInterval
	config.Offsets.CommitInterval = td.zkOffsetUpdateInterval
	td.cg, err = consumergroup.JoinConsumerGroup(
		td.moduleName,
		[]string{td.topic},
		td.zkHosts,
		config)
	if err != nil {
		return err
	}
	return nil
}

func (td *TransDi) initDispatchHash() (err error) {
	td.dispatchHasher = fnv.New32a()
	return nil
}

func (td *TransDi) initTransWorkers() (err error) {
	for id := uint32(0); id < td.workerNum; id++ {
		worker := &TransWorker{
			moduleName:        td.moduleName,
			workerId:          id,
			backendServers:    td.backendServers,
			protocolConfig:    td.protocolConfig,
			fatalErrorChan:    td.fatalErrorChan,
			inWork:            false,
			ackDataChan:       td.ackDataChan,
			maxRetryTimes:     td.maxRetryTimes,
			failRetryInterval: td.failRetryInterval,
		}
		err = worker.init()
		if err != nil {
			return
		}
		td.transWorkers = append(td.transWorkers, worker)
	}
	logger.Notice("all workers have been inited for moudle [%s]", td.moduleName)
	return nil
}

func (td *TransDi) waitAckNonBlock() {
	//	logger.Debug("in waitAckNonBlock")
	for {
		select {
		case ackData := <-td.ackDataChan:
			td.processAckData(ackData)
			/* do not break, so we can receive ack untile no ack, then state = CHECK_NEED_WAIT_ACK */
		default:
			td.state = CHECK_NEED_WAIT_ACK
			return
		}
	}
}

func (td *TransDi) waitAckBlock() {
	//	logger.Debug("in waitAckBlock")
	for ackData := range td.ackDataChan {
		td.processAckData(ackData)
		td.state = WAIT_ACK_NONBLOCK
		break
	}
}

func (td *TransDi) checkNeedWaitAck() {
	if td.transWindow.isFull() {
		td.state = WAIT_ACK_BLOCK
	} else {
		td.state = WAIT_ACK_AND_DI
	}
}

func (td *TransDi) waitAckAndDi() {
	//	logger.Debug("in waitAckAndDi")
	var ackData *AckData
	for {
		select {
		case ackData = <-td.ackDataChan:
			td.processAckData(ackData)
			td.state = WAIT_ACK_NONBLOCK
			return
		case message := <-td.cg.Messages():
			td.processConsumerMessage(message)
			return
		case err := <-td.cg.Errors():
			td.processConsumerError(err)
			return
		}
	}
	return
}

func (td *TransDi) processConsumerMessage(msg *sarama.ConsumerMessage) {
	/* unpack */
	/* pack {
	      'topic' => string,
		  'method' => string,
		  'key' => string,
		  'data' => []byte,
		  }
	*/
	// FIXME: need be optimized here
	sleep := 100 * time.Millisecond
	for {
		var err error
		method := gDefaultMethod
		if len(td.methodEnabled) > 0 {
			// if methods is configured, we should check if this method needed be send
			buf := bytes.NewReader(msg.Value)
			buf.Seek(0, 0)
			msgr := msgp.NewReader(buf)
			dii, err := msgr.ReadIntf()
			if err != nil {
				err = errors.New(fmt.Sprintf("fail to de-msgpack: %s", err.Error()))
				logger.Warning("fail to processConsumerMessage: topic=%s, partition=%d, transid=%d, error: %s", msg.Topic, msg.Partition, msg.Offset, err.Error())
				time.Sleep(sleep)
				continue
			}
			di, ok := dii.(map[string]interface{})
			if !ok {
				err = errors.New("invalid di: should be map")
				logger.Warning("fail to processConsumerMessage: topic=%s, partition=%d, transid=%d, error: %s", msg.Topic, msg.Partition, msg.Offset, err.Error())
				time.Sleep(sleep)
				continue
			}
			methodi, ok := di["method"]
			if !ok {
				err = errors.New(fmt.Sprintf("invalid di: method not exists"))
				logger.Warning("fail to processConsumerMessage: topic=%s, partition=%d, transid=%d, error: %s", msg.Topic, msg.Partition, msg.Offset, err.Error())
				time.Sleep(sleep)
				continue
			}
			method, ok = methodi.(string)
			if !ok {
				err = errors.New(fmt.Sprintf("invlid di: method is not string"))
				logger.Warning("fail to processConsumerMessage: topic=%s, partition=%d, transid=%d, error: %s", msg.Topic, msg.Partition, msg.Offset, err.Error())
				time.Sleep(sleep)
				continue
			}
			if _, ok = td.methodEnabled[method]; !ok {
				// not ours
				return
			}
		}
		// dispatch
		td.transWindow.addSlot(msg)
		var workerId uint32
		if msg.Key == nil || len(msg.Key) == 0 || !td.serializeByKey {
			workerId = uint32(rand.Int31n(int32(td.workerNum)))
		} else {
			td.dispatchHasher.Reset()
			_, err = td.dispatchHasher.Write(msg.Key)
			if err != nil {
				err = errors.New(fmt.Sprintf("fail to compute key-hash: %s", err.Error()))
				logger.Warning("fail to processConsumerMessage: topic=%s, partition=%d, transid=%d, error: %s", msg.Topic, msg.Partition, msg.Offset, err.Error())
				time.Sleep(sleep)
				continue
			}
			hash := td.dispatchHasher.Sum32()
			workerId = hash % td.workerNum
		}
		worker := td.transWorkers[workerId]
		transData := &TransData{
			transid:   msg.Offset,
			topic:     msg.Topic,
			method:    method,
			partition: msg.Partition,
			data:      msg.Value,
		}
		err = worker.addTrans(transData)
		if err != nil {
			logger.Warning("fail to processConsumerMessage: topic=%s, partition=%d, transid=%d, error: %s", msg.Topic, msg.Partition, msg.Offset, err.Error())
			time.Sleep(sleep)
			continue
		}
		td.state = WAIT_ACK_NONBLOCK
		return
	}
}

func (td *TransDi) processConsumerError(cerr *sarama.ConsumerError) {
	logger.Fatal("consumer error: topic=%s, partition=%d, %s", cerr.Topic, cerr.Partition, cerr.Error())
	td.fatalErrorChan <- cerr
}

/* do not change state in this func */
func (td *TransDi) processAckData(ackData *AckData) {
	transid := ackData.transid
	workerId := ackData.workerId
	logger.Debug("process ack: transid=%d, workerId=%d", transid, workerId)
	/* narrow window */
	msg := td.transWindow.ackOne(transid)
	if msg != nil && msg.Offset > td.ackedMinTransid {
		td.ackedMinTransid = msg.Offset
		err := td.cg.CommitUpto(msg)
		if err != nil {
			// TODO: optimized
			logger.Warning("fail to consumergroup.CommitUpto(): %s", err.Error())
			td.fatalErrorChan <- err
			return
		}
		logger.Debug("consumergroup.CommitUpTo %d", msg.Offset)
	}
	logger.Debug("transWindow size: %d", td.transWindow.window.Len())
	/* move worker */
	worker := td.transWorkers[workerId]
	worker.inWork = false
	err := worker.workIfNeed()
	if err != nil {
		logger.Fatal("fail to let worker to work: %s", err.Error())
		td.fatalErrorChan <- err
		return
	}
}
