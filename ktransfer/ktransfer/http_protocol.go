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
		"github.com/dzch/go-utils/logger"
		"net/http"
		"time"
		"errors"
		"fmt"
		"regexp"
		"strconv"
		"bytes"
	   )

type HttpProtocol struct {
	protocolName string
	moduleName string
	config map[interface{}]interface{}
	uri string
	header http.Header
	processTimeout time.Duration
	client *http.Client
	regTopic, regMethod, regPartition, regTransid *regexp.Regexp
}

func (hp *HttpProtocol) init() (err error) {
	logger.Debug("http protocol init")
	err = hp.initConfig()
	if err != nil {
		return
	}
	err = hp.initRegs()
	if err != nil {
		return
	}
	return nil
}

func (hp *HttpProtocol) name() string {
	return hp.protocolName
}

func (hp *HttpProtocol) transData(server string, transData *TransData) (err error) {
	/* compose req */
    uri := hp.regTopic.ReplaceAllString(hp.uri, transData.topic)
	uri = hp.regMethod.ReplaceAllString(uri, transData.method)
	uri = hp.regPartition.ReplaceAllString(uri, strconv.FormatInt(int64(transData.partition), 10))
	uri = hp.regTransid.ReplaceAllString(uri, strconv.FormatInt(transData.transid, 10))
    url := fmt.Sprintf("http://%s%s", server, uri)
	req, err := http.NewRequest("POST", url, bytes.NewReader(transData.data))
	if err != nil {
		logger.Warning("module [%s]: fail to transData: url=%s, topic=%s, partition=%d, transid=%d, method=%s, err=%s", hp.moduleName, url, transData.topic, transData.partition, transData.transid, transData.method, err.Error())
		return err
	}
	/* Post */
	res, err := hp.client.Do(req)
	if err != nil {
		logger.Warning("module [%s]: fail to transData: url=%s, topic=%s, partition=%d, transid=%d, method=%s, err=%s", hp.moduleName, url, transData.topic, transData.partition, transData.transid, transData.method, err.Error())
		return err
	}
	defer res.Body.Close()
	/* check res: 200 不重试；其他，重试 */
	if res.StatusCode == http.StatusOK {
		logger.Notice("module [%s]: success transData: url=%s, topic=%s, partition=%d, transid=%d, method=%s, datalen=%d, http_status_code=%d", hp.moduleName, url, transData.topic, transData.partition, transData.transid, transData.method, len(transData.data), res.StatusCode)
		return nil
	} else {
		logger.Warning("module [%s]: fail to transData: url=%s, topic=%s, partition=%d, transid=%d, method=%s, http_status_code=%d", hp.moduleName, url, transData.topic, transData.partition, transData.transid, transData.method, res.StatusCode)
		return errors.New("fail to trans")
	}
	return nil
}

func (hp *HttpProtocol) initConfig() (err error) {
	/* get uri */
	uri, ok := hp.config["uri"]
	if !ok {
		err = errors.New(fmt.Sprintf("fail to init HttpProtocol for module [%s]: uri not found", hp.moduleName))
		logger.Warning("%s", err.Error())
		return
	}
	hp.uri = uri.(string)
	/* get headers */
	/* a little trick, see http.Header */
	hp.header = http.Header(make(map[string][]string))
	headers, ok := hp.config["headers"]
	if ok {
		for _, header := range headers.([]interface{}) {
			for key, val := range header.(map[interface{}]interface{}) {
				hp.header.Add(key.(string), val.(string))
			}
		}
	}

    /* get timeout, 0 means no timeout */
	readTimeO, ok := hp.config["read_timeout_ms"]
	if !ok {
		err = errors.New(fmt.Sprintf("fail to init HttpProtocol for module [%s]: read_timeout_ms not found", hp.moduleName))
		logger.Warning("%s", err.Error())
		return
	}
	writeTimeO, ok := hp.config["write_timeout_ms"]
	if !ok {
		err = errors.New(fmt.Sprintf("fail to init HttpProtocol for module [%s]: write_timeout_ms not found", hp.moduleName))
		logger.Warning("%s", err.Error())
		return
	}
	connTimeO, ok := hp.config["conn_timeout_ms"]
	if !ok {
		err = errors.New(fmt.Sprintf("fail to init HttpProtocol for module [%s]: conn_timeout_ms not found", hp.moduleName))
		logger.Warning("%s", err.Error())
		return
	}
	if readTimeO == 0 || writeTimeO == 0 || connTimeO == 0 {
		hp.processTimeout = 0
	} else {
	    hp.processTimeout = time.Duration(readTimeO.(int) + writeTimeO.(int) + connTimeO.(int))*time.Millisecond
	}
	/* http client */
	hp.client = &http.Client {
        Timeout: hp.processTimeout,
	}
	return nil
}

func (hp *HttpProtocol) initRegs() (err error) {
	hp.regTopic, err = regexp.Compile("{#TOPIC}")
	if err != nil {
		logger.Warning("fail to regexp.Compile: %s", err.Error())
		return
	}
	hp.regMethod, err = regexp.Compile("{#METHOD}")
	if err != nil {
		logger.Warning("fail to regexp.Compile: %s", err.Error())
		return
	}
	hp.regPartition, err = regexp.Compile("{#PARTITION}")
	if err != nil {
		logger.Warning("fail to regexp.Compile: %s", err.Error())
		return
	}
	hp.regTransid, err = regexp.Compile("{#TRANSID}")
	if err != nil {
		logger.Warning("fail to regexp.Compile: %s", err.Error())
		return
	}
	return nil
}
