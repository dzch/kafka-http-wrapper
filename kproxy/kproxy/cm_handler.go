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
		"net/http"
		"time"
		"io"
	   )

type CmHandler struct {
	cdp *CmDataPool
	kp *KProxy
}

func newCmHandler(kp *KProxy) (*CmHandler, error) {
    ch := &CmHandler {
	    kp: kp,
	}
    err := ch.init()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (ch *CmHandler) init() error {
    err := ch.initCDP()
	if err != nil {
		return err
	}
	return nil
}

func (ch *CmHandler) initCDP() error {
	ch.cdp = newCmDataPool(ch.kp.config.cmDataPoolSize)
	return nil
}

func (ch *CmHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    startTime := time.Now()
	/* check query */
	if r.ContentLength <= 0 {
		logger.Warning("invalid query, need post data: %s", r.URL.String())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
    qv := r.URL.Query()
    post := make([]byte, r.ContentLength)
	nr, err := io.ReadFull(r.Body, post)
	if int64(nr) != r.ContentLength {
		logger.Warning("fail to read body: %s, %s", r.URL.String(), err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	/* compose CmData */
	cmData := ch.cdp.fetch()
	defer ch.cdp.put(cmData)
	switch qv.Get("acks") {
		case "0":
			cmData.requiredAcks = sarama.NoResponse
		case "1":
			cmData.requiredAcks = sarama.WaitForLocal
		case "-1":
			cmData.requiredAcks = sarama.WaitForAll
		default:
			cmData.requiredAcks = sarama.WaitForLocal
	}
	cmData.topic = qv.Get("topic")
	cmData.key = qv.Get("key")
	cmData.data = post
	/* commit */
	ch.kp.producer.produce(cmData)
	/* wait res */
	<-cmData.cmDoneChan
	if cmData.err != nil {
		logger.Warning("fail to commit req: %s, error: %s", r.URL.String(), (*(cmData.err)).Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
    endTime := time.Now()
	costTimeUS := endTime.Sub(startTime)/time.Microsecond
	// TODO
	logger.Notice("success process commit: %s, cost_us=%d, datalen=%d, offset=%d, partition=%d", r.URL.String(), costTimeUS, nr, cmData.offset, cmData.partition)
	w.WriteHeader(http.StatusOK)
	return
}
