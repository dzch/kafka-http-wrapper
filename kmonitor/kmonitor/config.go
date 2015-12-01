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
		"github.com/go-yaml/yaml"
		"io/ioutil"
		"errors"
		"fmt"
		"time"
	   )

type HttpServerConfig struct {
	port int
	readTimeout time.Duration
	writeTimeout time.Duration
}

type ClusterConfig struct {
    clusterName string
	chroot string
	zkHosts []string
	zkSessionTimeout time.Duration
	zkConnectTimeout time.Duration
	//modules []string
}

type BaseConfig struct {
	logDir string
	logLevel int
	clusters []string
}

type Config struct {
	confFile string
	m map[interface{}]interface{}
	bc *BaseConfig
	cc map[string]*ClusterConfig
	hsc *HttpServerConfig
}

func newConfig(confFile string) (*Config, error) {
    c := &Config {
		confFile: confFile,
		m: make(map[interface{}]interface{}),
		cc: make(map[string]*ClusterConfig),
		hsc: &HttpServerConfig{},
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
	err = yaml.Unmarshal(content, &c.m)
	if err != nil {
		return err
	}
	err = c.initBaseConfig()
	if err != nil {
		return err
	}
	err = c.initClusterConfig()
	if err != nil {
		return err
	}
	err = c.initHttpServerConfig()
	if err != nil {
		return err
	}
	c.dump()
	return nil
}

func (c *Config) initBaseConfig() error {
	base, ok := c.m["base"]
	if !ok {
		return errors.New("base not found in conf file")
	}
	m, ok := base.(map[interface{}]interface{})
	if !ok {
		return errors.New("base not a map")
	}
	bc := &BaseConfig{}
    logDir, ok := m["log_dir"]
	if !ok {
		return errors.New("base: log_dir not found in conf file")
    }
	bc.logDir = logDir.(string)
	logLevel, ok := m["log_level"]
	if !ok {
		return errors.New("base: log_level not found in conf file")
	}
	bc.logLevel = logLevel.(int)
	clustersi, ok := m["clusters"]
	if !ok {
		return errors.New("base: clusters not found in conf file")
	}
	clusters, ok := clustersi.([]interface{})
	if !ok {
		return errors.New("base: clusters should be array")
	}
	for _, val := range clusters {
        cluster, ok := val.(string)
        if !ok {
			return errors.New("base: element in clusters should be string")
		}
		bc.clusters = append(bc.clusters, cluster)
	}
	if len(bc.clusters) == 0 {
		return errors.New("base: clusters has no element")
	}
	c.bc = bc
	return nil
}

func (c *Config) initClusterConfig() error {
	for _, clusterName := range c.bc.clusters {
	    clusteri, ok := c.m[clusterName]
	    if !ok {
	        return errors.New(fmt.Sprintf("%s not found in conf file", clusterName))
	    }
	    m, ok := clusteri.(map[interface{}]interface{})
	    if !ok {
		    return errors.New(fmt.Sprintf("%s not a map", clusterName))
	    }
	    cc := &ClusterConfig{}
		cc.clusterName = clusterName
	    chroot, ok := m["chroot"]
	    if !ok {
		    return errors.New(fmt.Sprintf("%s: chroot not found in conf file", clusterName))
	    }
		cc.chroot = chroot.(string)
	    zkHostsi, ok := m["zk_hosts"]
	    if !ok {
		    return errors.New(fmt.Sprintf("%s: zk_hosts not found in conf file", clusterName))
	    }
		zkHosts, ok := zkHostsi.([]interface{})
		if !ok {
		    return errors.New(fmt.Sprintf("%s: zk_hosts not array", clusterName))
	    }
		for _, zkHost := range zkHosts {
			cc.zkHosts = append(cc.zkHosts, zkHost.(string))
		}
		if len(cc.zkHosts) == 0 {
			return errors.New(fmt.Sprintf("%s: zk_hosts has no element", clusterName))
		}
		timeout, ok := m["zk_connect_timeout_ms"]
		if !ok {
			return errors.New(fmt.Sprintf("%s: zk_connect_timeout_ms not found in conf file", clusterName))
		}
		cc.zkConnectTimeout = time.Duration(timeout.(int)) * time.Millisecond
		timeout, ok = m["zk_session_timeout_ms"]
		if !ok {
			return errors.New(fmt.Sprintf("%s: zk_session_timeout_ms not found in conf file", clusterName))
		}
		cc.zkSessionTimeout = time.Duration(timeout.(int)) * time.Millisecond
//	    modulesi, ok := m["modules"]
//	    if !ok {
//		    return errors.New(fmt.Sprintf("%s: modules not found in conf file", clusterName))
//	    }
//		modules, ok := modulesi.([]interface{})
//		if !ok {
//		    return errors.New(fmt.Sprintf("%s: modules not array", clusterName))
//	    }
//		for _, module := range modules {
//			cc.modules = append(cc.modules, module.(string))
//		}
//		if len(cc.modules) == 0 {
//			return errors.New(fmt.Sprintf("%s: modules has no element", clusterName))
//		}
		c.cc[clusterName] = cc
	}
	return nil
}

func (c *Config) initHttpServerConfig() error {
	hs, ok := c.m["http_server"]
	if !ok {
		return errors.New("http_server not found in conf file")
	}
	m, ok := hs.(map[interface{}]interface{})
	if !ok {
		return errors.New("http_server not a map")
	}
	hsc := &HttpServerConfig{}
    port, ok := m["port"]
	if !ok {
		return errors.New("http_server: port not found in conf file")
	}
	hsc.port = port.(int)
	timeo, ok := m["read_timeout_ms"]
	if !ok {
		return errors.New("http_server: read_timeout_ms not found in conf file")
	}
	hsc.readTimeout = time.Duration(timeo.(int))*time.Millisecond
	timeo, ok = m["write_timeout_ms"]
	if !ok {
		return errors.New("http_server: write_timeout_ms not found in conf file")
	}
	hsc.writeTimeout = time.Duration(timeo.(int))*time.Millisecond
	c.hsc = hsc
	return nil
}

func (c *Config) dump() {
	fmt.Println("confFile:", c.confFile)
}

