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
//		"github.com/dzch/go-utils/logger"
		"github.com/Shopify/sarama"
		"time"
	   )

var (
		gCmDataChanSize = 64
	)

type Producer struct {
	config *Config
	fatalErrorChan chan *error
	tpNoResponse, tpWaitForLocal, tpWaitForAll *TypedProducer
}

type TypedProducer struct {
    pmp *PMsgPool // Be carefull, NO lock !
	cmChan chan *CmData
	requiredAcks sarama.RequiredAcks
	ap sarama.AsyncProducer
	fatalErrorChan chan *error
}

func newProducer(kp *KProxy) (*Producer, error) {
    p := &Producer {
            config: kp.config,
		    fatalErrorChan: kp.fatalErrorChan,
	   }
	err := p.init()
	if err != nil {
		return nil, err
	}
    return p, nil
}

func (p *Producer) init() error {
	err := p.initAllProducers()
	if err != nil {
		return err
	}
	return nil
}

func (p *Producer) initAllProducers() error {
	var err error
	sarama.Logger = newSaramaLogger()
    p.tpNoResponse, err = p.newTypedProducer(sarama.NoResponse)
	if err != nil {
		return err
	}
    p.tpWaitForLocal, err = p.newTypedProducer(sarama.WaitForLocal)
	if err != nil {
		return err
	}
    p.tpWaitForAll, err = p.newTypedProducer(sarama.WaitForAll)
	if err != nil {
		return err
	}
	return nil
}

func (p *Producer) run() {
	go p.tpNoResponse.run()
	go p.tpWaitForLocal.run()
	go p.tpWaitForAll.run()
}

func (p *Producer) produce(cmData *CmData) {
	switch cmData.requiredAcks {
		case sarama.NoResponse:
			p.tpNoResponse.cmChan <-cmData
		case sarama.WaitForLocal:
			p.tpWaitForLocal.cmChan <-cmData
	    case sarama.WaitForAll:
			p.tpWaitForAll.cmChan <-cmData
	}
}

func (p *Producer) newTypedProducer(requiredAcks sarama.RequiredAcks) (*TypedProducer, error) {
    tp := &TypedProducer {
        pmp : newPMsgPool(p.config.producerMsgPoolSize),
		cmChan: make(chan *CmData, gCmDataChanSize),
		requiredAcks: requiredAcks,
		fatalErrorChan: p.fatalErrorChan,
	}
    pconfig := sarama.NewConfig()
	pconfig.Net.MaxOpenRequests = 10240
	pconfig.Metadata.RefreshFrequency = 10*time.Second
	pconfig.Producer.MaxMessageBytes = p.config.producerMaxMessageSize
	pconfig.Producer.RequiredAcks = requiredAcks
	pconfig.Producer.Return.Successes = true
	pconfig.Producer.Return.Errors = true
	pconfig.Producer.Partitioner = sarama.NewHashPartitioner
	var err error
	tp.ap, err = sarama.NewAsyncProducer(p.config.brokerList, pconfig)
	if err != nil {
		return nil, err
	}
    return tp, nil
}

func (tp *TypedProducer) run() {
	defer tp.ap.Close()
	for {
	    select {
		case cmData := <-tp.cmChan:
			tp.produce(cmData)
		case perr := <-tp.ap.Errors():
			tp.processProduceErrors(perr)
		case psucc := <-tp.ap.Successes():
			tp.processProduceSuccesses(psucc)
	    }
	}
}

func (tp *TypedProducer) produce(cmData *CmData) {
	// logger.Debug("produce requiredAcks=%d", int(tp.requiredAcks))
	// fetch and fill
    pmpe := tp.pmp.fetch()
	pmpe.privData = cmData
	pmsg := pmpe.pmsg
	pmsg.Topic = cmData.topic
	if len(cmData.key) == 0 {
		// if key is empty, using sarama.RandomPartitioner
		pmsg.Key = nil
	} else {
	    pmsg.Key = sarama.StringEncoder(cmData.key)
	}
	pmsg.Value = sarama.ByteEncoder(cmData.data)
	pmsg.Metadata = pmpe
	// do produce
	for {
		select {
			case tp.ap.Input() <-pmsg:
				return
			case perr := <-tp.ap.Errors():
				tp.processProduceErrors(perr)
		}
	}
}

func (tp *TypedProducer) processProduceErrors(perr *sarama.ProducerError) {
	// fetch
    pmsg := perr.Msg
	pmpe := pmsg.Metadata.(*PMsgPoolEle)
	cmData := pmpe.privData.(*CmData)
	// put
	tp.pmp.put(pmpe)
	// notice
	cmData.err = &perr.Err
	cmData.cmDoneChan <- 1
}

func (tp *TypedProducer) processProduceSuccesses(psucc *sarama.ProducerMessage) {
	// fetch
    pmpe := psucc.Metadata.(*PMsgPoolEle)
	cmData := pmpe.privData.(*CmData)
	// put
	tp.pmp.put(pmpe)
	// notice
	cmData.err = nil
	cmData.offset = psucc.Offset
	cmData.partition = psucc.Partition
	cmData.cmDoneChan <- 1
}
