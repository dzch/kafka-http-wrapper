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
		"github.com/go-yaml/yaml"
		"time"
		"io/ioutil"
		"errors"
		"fmt"
	   )

type Config struct {
	// kproxy
	confFile string
	logDir string
	logLevel int
	// http server
	httpServerListenPort uint16
	httpServerReadTimeout time.Duration
	httpServerWriteTimeout time.Duration
	cmDataPoolSize int
	// producer
	brokerList []string
	producerMsgPoolSize int
	producerMaxMessageSize int
}

func newConfig(confFile string) (*Config, error) {
    c := &Config {
		confFile: confFile,
	   }
	err := c.init()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) init() error {
    content, err := ioutil.ReadFile(c.confFile)
	if err != nil {
		return err
	}
    m := make(map[interface{}]interface{})
	err = yaml.Unmarshal(content, &m)
	if err != nil {
		return err
	}

	/* log conf */
    logDir, ok := m["log_dir"]
	if !ok {
		return errors.New("log_dir not found in conf file")
    }
	c.logDir = logDir.(string)
	logLevel, ok := m["log_level"]
	if !ok {
		return errors.New("log_level not found in conf file")
	}
	c.logLevel = logLevel.(int)

	/* broker conf */
	brokerhosts, ok := m["broker_hosts"]
	if !ok {
		return errors.New("broker_hosts not found in conf file")
	}
    brokerHosts := brokerhosts.([]interface{})
	if len(brokerHosts) <= 0 {
		return errors.New("num of brokerHosts is zero")
	}
	for _, brokerhost := range brokerHosts {
		c.brokerList = append(c.brokerList, brokerhost.(string))
	}

	/* producer conf */
	pmms, ok := m["max_request_size_bytes"]
	if !ok {
		return errors.New("max_request_size_bytes not found in conf file")
	}
	c.producerMaxMessageSize = pmms.(int)

	/* http server */
	port, ok := m["port"]
	if !ok {
		return errors.New("port not found in conf file")
	}
	c.httpServerListenPort = uint16(port.(int))
	rtimeo, ok := m["read_timeout_ms"]
	if !ok {
		return errors.New("read_timeout_ms not found in conf file")
	}
	c.httpServerReadTimeout = time.Duration(rtimeo.(int))*time.Millisecond
	wtimeo, ok := m["write_timeout_ms"]
	if !ok {
		return errors.New("write_timeout_ms not found in conf file")
	}
	c.httpServerWriteTimeout = time.Duration(wtimeo.(int))*time.Millisecond
	reqPoolSize, ok := m["req_pool_size"]
	if !ok {
		c.cmDataPoolSize = 10240
		c.producerMsgPoolSize = 10240
	} else {
		c.cmDataPoolSize = reqPoolSize.(int)
		c.producerMsgPoolSize = reqPoolSize.(int)
	}

	c.dump()
	return nil
}

func (c *Config) dump() {
	fmt.Println("confFile:", c.confFile)
	fmt.Println("logDir:", c.logDir)
	fmt.Println("logLevel:", c.logLevel)
	fmt.Println("httpServerListenPort:", c.httpServerListenPort)
	fmt.Println("httpServerReadTimeout:", c.httpServerReadTimeout)
	fmt.Println("httpServerWriteTimeout:", c.httpServerWriteTimeout)
	fmt.Println("cmDataPoolSize:", c.cmDataPoolSize)
	fmt.Println("producerMsgPoolSize:", c.producerMsgPoolSize)
	for idx, broker := range c.brokerList {
		fmt.Println("broker ", idx, broker)
	}
}

