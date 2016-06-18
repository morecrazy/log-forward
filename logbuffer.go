package main

import (
	"bytes"
	"sync"
	"backend/common"
)
type LogBuffer struct {
	m *sync.Mutex
	buf *bytes.Buffer
	len int64
	linePrefix string
	ch chan bool
	broker string
	name string
}

func (b *LogBuffer) WriteString(s string) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	//common.Logger.Debug("start write string to logbuffer, the log buffer name is %s, and the length is %d", b.broker, b.len)
	b.len ++
	if b.len == gLogBufferSize {
		b.ch <- true
	}
	line := b.linePrefix + s
	b.linePrefix = "\n"
	return b.buf.WriteString(line)
}

func (b *LogBuffer) ReadString() string {
	b.m.Lock()
	defer b.m.Unlock()
	str := b.buf.String()
	common.Logger.Debug("start read string from logbuffer, the log buffer name is %s, and the length is %d", b.broker, b.len)
	b.buf.Reset()
	b.len = 0
	b.linePrefix = ""
	return str
}

func newLogBuffer(fileName , broker string) *LogBuffer {
	common.Logger.Debug("Creating a new logbuffer")
	logBuffer := new(LogBuffer)
	logBuffer.broker = broker
	logBuffer.name = fileName
	logBuffer.buf = new(bytes.Buffer)
	logBuffer.m = new(sync.Mutex)
	logBuffer.ch = make(chan bool, 1)
	logBuffer.len = 0
	logBuffer.linePrefix = ""
	//每创建一个logbuffer,同事创建一个forwarder读取buffer数据
	go forwarder(logBuffer)
	return logBuffer
}