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
		"github.com/Shopify/sarama"
		"github.com/dzch/go-utils/logger"
		"container/ring"
	   )

type PMsgPoolEle struct {
	pmsg *sarama.ProducerMessage
	privData interface{}
	r *ring.Ring
}

// no lock here
// should only be called by one routine
type PMsgPool struct {
	root *PMsgPoolEle
	poolSize int
	poolFreeSize int
}

func newPMsgPool(size int) *PMsgPool {
    pmp := &PMsgPool {
            poolSize: size,
		 }
	pmp.init()
	return pmp
}

func (pmp *PMsgPool) init() {
	pmp.root = pmp.newPMsgPoolEle()
	for i := 0; i < pmp.poolSize; i ++ {
        pmpe := pmp.newPMsgPoolEle()
		pmp.root.r.Link(pmpe.r)
	}
	pmp.poolFreeSize = pmp.poolSize
}

func (pmp *PMsgPool) newPMsgPoolEle() *PMsgPoolEle {
    pmpe := &PMsgPoolEle {
        pmsg: &sarama.ProducerMessage{},
		r: ring.New(1),
		  }
	pmpe.r.Value = pmpe
	return pmpe
}

func (pmp *PMsgPool) fetch() *PMsgPoolEle {
	if pmp.poolFreeSize <= 0 {
		logger.Warning("Producer Message Pool is too small");
		return pmp.newPMsgPoolEle()
	}
    r := pmp.root.r.Unlink(1)
	pmp.poolFreeSize --
	return r.Value.(*PMsgPoolEle)
}

func (pmp *PMsgPool) put(pmpe *PMsgPoolEle) {
	if pmp.poolFreeSize >= pmp.poolSize {
		return
	}
	pmp.root.r.Link(pmpe.r)
	pmp.poolFreeSize ++
	return
}

