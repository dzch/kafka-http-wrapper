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
		"github.com/samuel/go-zookeeper/zk"
		"net"
		"time"
		"fmt"
		"errors"
		"encoding/json"
		"strconv"
	   )

type BrokerIdVal struct {
	Host string `json:"host"`
	Port int `json:"port"`
}

type TransferOffset struct {
	cg string
	topic string
	partition int32
	offset int64
}

type Zookeeper struct {
	cc *ClusterConfig
	zkConn *zk.Conn
    zkEventChan <-chan zk.Event
	brokerIdValChan chan *BrokerIdVal
	transferOffsetChan chan *TransferOffset
	fatalErrorChan chan error
}

func newZookeeper(c *Cluster) (*Zookeeper, error) {
    z := &Zookeeper{
		cc: c.cc,
		brokerIdValChan: c.brokerIdValChan,
		transferOffsetChan: c.transferOffsetChan,
		fatalErrorChan: c.fatalErrorChan,
	}
	err := z.init()
	if err != nil {
		return nil, err
	}
	return z, nil
}

func (z *Zookeeper) init() error {
    err := z.initConn()
	if err != nil {
		return err
	}
	return nil
}

// zk dialer
func (z *Zookeeper) zkDialer(network, address string, timeout time.Duration) (net.Conn, error) {
    timeout = z.cc.zkConnectTimeout
	return net.DialTimeout(network, address, timeout)
}

// connect zk and do eventloop
func (z *Zookeeper) initConn() error {
	var err error
	z.zkConn, z.zkEventChan, err = zk.ConnectWithDialer(z.cc.zkHosts, z.cc.zkSessionTimeout, z.zkDialer)
	if err != nil {
		return err
	}
	z.zkConn.SetLogger(fakeLogger)
	return nil
}

func (z *Zookeeper) run() {
	go z.updateBrokerListAndWatch()
	go z.updateTransferAndWatch()
}

func (z *Zookeeper) updateBrokerListAndWatch() {
	for {
	    children, _, eventChan, err := z.zkConn.ChildrenW(fmt.Sprintf("%s/brokers/ids", z.cc.chroot))
	    if err != nil {
		    err = errors.New(fmt.Sprintf("fail to zk.ChildrenW: %s", err.Error()))
		    logger.Fatal(err.Error())
		    z.fatalErrorChan <-err
			return
	    }
	    if len(children) == 0 {
		    logger.Warning("no broker found")
	    } else {
	        logger.Debug("using child: %s", children[0])
	        val, _, err := z.zkConn.Get(fmt.Sprintf("%s/brokers/ids/%s", z.cc.chroot, children[0]))
	        var brokerIdVal BrokerIdVal
	        err = json.Unmarshal(val, &brokerIdVal)
	        if err != nil {
		        logger.Warning("fail to json.Unmarshal for broker id %s: %s", children[0], err.Error())
	        }
	        logger.Debug("broker id %s: host [%s] port [%d]", children[0], brokerIdVal.Host, brokerIdVal.Port)
			z.brokerIdValChan<-&brokerIdVal
		}
		<-eventChan
	}
}

func (z *Zookeeper) updateTransferAndWatch() {
    path :=fmt.Sprintf("%s/consumers", z.cc.chroot)
	for {
		children, _, eventChan, err := z.zkConn.ChildrenW(path)
		if err != nil {
			err = errors.New(fmt.Sprintf("fail to zk.ChildrenW: path=%s, %s", path, err.Error()))
			logger.Fatal(err.Error())
			z.fatalErrorChan <-err
			return
		}
		if len(children) == 0 {
			logger.Warning("no transfer cg found: path=%s", path)
		} else {
			for _, cg := range children {
				go z.updateCgAndWatch(cg)
			}
		}
		<-eventChan
	}
}

func (z *Zookeeper) updateCgAndWatch(cg string) {
    path :=fmt.Sprintf("%s/consumers/%s/offsets", z.cc.chroot, cg)
	for {
		children, _, eventChan, err := z.zkConn.ChildrenW(path)
		if err == zk.ErrNoNode {
			_, _, eventChan, err = z.zkConn.ExistsW(path)
		    if err != nil {
			    err = errors.New(fmt.Sprintf("fail to zk.ExistsW: path=%s, %s", path, err.Error()))
			    logger.Fatal(err.Error())
			    z.fatalErrorChan <-err
			    return
		    }
			<-eventChan
			continue
		}
		if err != nil {
			err = errors.New(fmt.Sprintf("fail to zk.ChildrenW: path=%s, %s", path, err.Error()))
			logger.Fatal(err.Error())
			z.fatalErrorChan <-err
			return
		}
		if len(children) == 0 {
			logger.Warning("no transfer topic found: path=%s", path)
		} else {
			for _, topic := range children {
				go z.updateTopicAndWatch(cg, topic)
			}
		}
        event := <-eventChan
		// let root watcher to re-init
		if event.Type == zk.EventNotWatching {
			return
		}
	}
}

func (z *Zookeeper) updateTopicAndWatch(cg, topic string) {
    path :=fmt.Sprintf("%s/consumers/%s/offsets/%s", z.cc.chroot, cg, topic)
	for {
		children, _, eventChan, err := z.zkConn.ChildrenW(path)
		if err == zk.ErrNoNode {
			_, _, eventChan, err = z.zkConn.ExistsW(path)
		    if err != nil {
			    err = errors.New(fmt.Sprintf("fail to zk.ExistsW: path=%s, %s", path, err.Error()))
			    logger.Fatal(err.Error())
			    z.fatalErrorChan <-err
			    return
		    }
			<-eventChan
			continue
		}
		if err != nil {
			err = errors.New(fmt.Sprintf("fail to zk.ChildrenW: path=%s, %s", path, err.Error()))
			logger.Fatal(err.Error())
			z.fatalErrorChan <-err
			return
		}
		if len(children) == 0 {
			logger.Warning("no transfer partition found: path=%s", path)
		} else {
			for _, partition := range children {
				p, err := strconv.ParseInt(partition, 10, 64)
				if err != nil {
				    err = errors.New(fmt.Sprintf("fail to transfer: strconv.ParseInt: %s", err.Error()))
				    logger.Fatal(err.Error())
				    z.fatalErrorChan <-err
					return
				}
				go z.updatePartitionAndWatch(cg, topic, int32(p))
			}
		}
        event := <-eventChan
		// let root watcher to re-init
		if event.Type == zk.EventNotWatching {
			return
		}
	}
}

func (z *Zookeeper) updatePartitionAndWatch(cg, topic string, partition int32) {
    path :=fmt.Sprintf("%s/consumers/%s/offsets/%s/%d", z.cc.chroot, cg, topic, partition)
	for {
		val, _, eventChan, err := z.zkConn.GetW(path)
		if err == zk.ErrNoNode {
			_, _, eventChan, err = z.zkConn.ExistsW(path)
		    if err != nil {
			    err = errors.New(fmt.Sprintf("fail to zk.ExistsW: path=%s, %s", path, err.Error()))
			    logger.Fatal(err.Error())
			    z.fatalErrorChan <-err
			    return
		    }
			<-eventChan
			continue
		}
		if err != nil {
		    err = errors.New(fmt.Sprintf("fail to zk.GetW: path=%s, %s", path, err.Error()))
		    logger.Fatal(err.Error())
		    z.fatalErrorChan <-err
		    return
		}
		if len(val) == 0 {
			logger.Warning("transfer partition val is empty: path=%s", path)
		} else {
			offset, err := strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				err = errors.New(fmt.Sprintf("fail to transfer: strconv.ParseInt: %s", err.Error()))
				logger.Fatal(err.Error())
				z.fatalErrorChan <-err
				return
			}
            z.transferOffsetChan <-&TransferOffset {
                cg: cg,
				topic: topic,
				partition: partition,
				offset: offset,
			}
	        logger.Debug("see transfer: cg=%s, topic=%s, partition=%d, offset=%d", cg, topic, partition, offset)
	    }
        event := <-eventChan
		// let root watcher to re-init
		if event.Type == zk.EventNotWatching {
			return
		}
	}
}

