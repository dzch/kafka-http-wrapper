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
		"time"
		"strconv"
	   )


type TransferRecord struct {
	offset int64
	timestamp int64
}

type TransferOffsetRecord struct {
	last TransferRecord
}

type TransferStatus struct {
	transferOffset map[string]map[string]map[int32]*TransferOffsetRecord
}

type BrokerRecord struct {
	offset int64
	timestamp int64
}

type BrokerOffsetRecord struct {
	last, lastBefore BrokerRecord
}

type BrokerStatus struct {
	brokerOffset map[string]map[int32]*BrokerOffsetRecord
}

type ClusterStatus struct {
	broker *BrokerStatus
	transfer *TransferStatus
}

type TransDelay map[string]map[string]int64

type Cluster struct {
	cc *ClusterConfig
	z *Zookeeper
	broker *Broker
	fatalErrorChan chan error
	brokerIdValChan chan *BrokerIdVal
	transferOffsetChan chan *TransferOffset
	brokerOffsetChan chan *BrokerOffset
	transDelayChan chan chan TransDelay
	cs *ClusterStatus
}

func newCluster(km *KMonitor, name string) (*Cluster, error) {
    c := &Cluster {
		cc: km.config.cc[name],
		fatalErrorChan: km.fatalErrorChan,
		brokerOffsetChan: make(chan *BrokerOffset, 4),
		transferOffsetChan: make(chan *TransferOffset, 4),
		brokerIdValChan: make(chan *BrokerIdVal),
		transDelayChan: make(chan chan TransDelay),
	}
	err := c.init()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cluster) run() {
	go c.z.run()
	go c.broker.run()
	for {
		select {
            case bo := <-c.brokerOffsetChan:
				c.updateBrokerOffset(bo)
			case to := <-c.transferOffsetChan:
				c.updateTransferOffset(to)
			case transDelayChan := <-c.transDelayChan:
				c.computeTransDelay(transDelayChan)
		}
	}
}

func (c *Cluster) init() error {
	c.initClusterStatus()
    err := c.initZookeeper()
	if err != nil {
		return err
	}
    err = c.initBroker()
	if err != nil {
		return err
	}
	return nil
}

func (c *Cluster) initClusterStatus() {
    cs := &ClusterStatus{}
	cs.broker = &BrokerStatus{}
	cs.broker.brokerOffset = make(map[string]map[int32]*BrokerOffsetRecord)
	cs.transfer = &TransferStatus{}
	cs.transfer.transferOffset = make(map[string]map[string]map[int32]*TransferOffsetRecord)
	c.cs = cs
}

func (c *Cluster) initZookeeper() error {
	var err error
	c.z, err = newZookeeper(c)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cluster) initBroker() error {
	var err error
	c.broker, err = newBroker(c)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cluster) updateBrokerOffset(bo *BrokerOffset) {
    pm, ok := c.cs.broker.brokerOffset[bo.topic]
	if !ok {
        pm = make(map[int32]*BrokerOffsetRecord)
		c.cs.broker.brokerOffset[bo.topic] = pm
	}
	bor, ok := pm[bo.partition]
	if !ok {
		bor = &BrokerOffsetRecord{}
		pm[bo.partition] = bor
	}
	bor.lastBefore = bor.last
	bor.last.offset = bo.offset
	bor.last.timestamp = time.Now().Unix()
}

func (c *Cluster) updateTransferOffset(to *TransferOffset) {
	cg, ok := c.cs.transfer.transferOffset[to.cg]
	if !ok {
		cg = make(map[string]map[int32]*TransferOffsetRecord)
		c.cs.transfer.transferOffset[to.cg] = cg
	}
	tp, ok := cg[to.topic]
	if !ok {
		tp = make(map[int32]*TransferOffsetRecord)
		cg[to.topic] = tp
	}
	r, ok := tp[to.partition]
	if !ok {
		r = &TransferOffsetRecord {}
		tp[to.partition] = r
	}
	r.last.offset = to.offset
	r.last.timestamp = time.Now().Unix()
}

// called from other goroutine
func (c *Cluster) getTransDelay() TransDelay {
    resChan := make(chan TransDelay, 1)
	c.transDelayChan <-resChan
	return <-resChan
}

// smooth broker offset
func (c *Cluster) computeBrokerOffsetAtTime(timestamp int64, bor *BrokerOffsetRecord) int64 {
	if bor.last.timestamp == bor.lastBefore.timestamp {
		return bor.last.offset
	}
	return bor.lastBefore.offset + (timestamp - bor.lastBefore.timestamp) * (bor.last.offset - bor.lastBefore.offset) / (bor.last.timestamp - bor.lastBefore.timestamp)
}

func (c *Cluster) computeTransDelay(transDelayChan chan TransDelay) {
    resMap := make(map[string]map[string]int64)
	for cg, tm := range c.cs.transfer.transferOffset {
		for topic, pm := range tm {
			dpm, ok := resMap[cg]
			if !ok {
				dpm = make(map[string]int64)
				resMap[cg] = dpm
			}
			for partition, tor := range pm {
                bor, ok := c.cs.broker.brokerOffset[topic][partition]
				if !ok {
					logger.Warning("no broker offset record found: topic=%s, partition=%d", topic, partition)
					dpm[strconv.FormatInt(int64(partition), 10)] = 0
					continue
				}
                bo := c.computeBrokerOffsetAtTime(tor.last.timestamp, bor)
				delay := bo - tor.last.offset
				if delay < 0 {
					delay = 0
				}
				dpm[strconv.FormatInt(int64(partition), 10)] = delay
			}
		}
	}
	transDelayChan <-TransDelay(resMap)
}

