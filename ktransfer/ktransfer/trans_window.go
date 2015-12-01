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
		"gopkg.in/Shopify/sarama.v1"
		"container/list"
	   )

type WindowSlot struct {
	acked bool
	transid int64
	msg *sarama.ConsumerMessage
}

type TransWindow struct {
	windowSize int
	window *list.List
	slots map[int64]*WindowSlot
}

func (w *TransWindow) init() (err error) {
	w.window = list.New()
	w.slots = make(map[int64]*WindowSlot)
	return nil
}

func (w *TransWindow) isFull() bool {
	return w.windowSize == w.window.Len()
}

func (w *TransWindow) addSlot (msg *sarama.ConsumerMessage) {
    slot := &WindowSlot {
        acked: false,
		transid: msg.Offset,
		msg: msg,
	}
	w.window.PushBack(slot)
	w.slots[msg.Offset] = slot
}

func (w *TransWindow) ackOne(transid int64) (*sarama.ConsumerMessage) {
    slot, ok := w.slots[transid]
	if ok {
		slot.acked = true
	}
	return w.narrowWindow()
}

func (w *TransWindow) narrowWindow() (msg *sarama.ConsumerMessage) {
	for e := w.window.Front(); e != nil; /* empty */ {
        slot := e.Value.(*WindowSlot)
		if !slot.acked {
			return
		}
		tmp := e.Next()
		msg = slot.msg
		w.window.Remove(e)
		delete(w.slots, slot.transid)
		e = tmp
	}
	return
}


