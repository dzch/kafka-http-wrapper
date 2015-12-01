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
		"github.com/Shopify/sarama"
	   )

type CmData struct {
	// cm
	topic string
	key string
	data []byte
	// res
	requiredAcks sarama.RequiredAcks
	cmDoneChan chan int
	err *error
	offset int64
	partition int32
}

type CmDataPool struct {
	pool chan *CmData
}

func newCmDataPool(size int) *CmDataPool {
    cdp := &CmDataPool {
        pool: make(chan *CmData, size),
		 }
init:
	for {
		select {
			case cdp.pool <- cdp.newCmData():
			default:
				break init
		}
	}
	return cdp
}

func (cdp *CmDataPool) fetch() *CmData {
	var cd *CmData
	select {
		case cd = <-cdp.pool:
		default:
			logger.Warning("cm_data_pool is not big enough")
			cd = cdp.newCmData()
	}
	return cd
}

func (cdp *CmDataPool) put(cd *CmData) {
	select {
		case cdp.pool <-cd:
		default:
			// let it go
	}
}

func (cdp *CmDataPool) newCmData() *CmData {
    cmData := &CmData {
		cmDoneChan: make(chan int, 1),
    }
	return cmData
}

