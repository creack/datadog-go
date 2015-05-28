package statsd

import (
	"io"
	"io/ioutil"
	"testing"
	"time"
)

type NopWriteCloser struct {
	io.Writer
}

func (*NopWriteCloser) Close() error { return nil }

func NewNopClient() *Client {
	return &Client{
		conn: &NopWriteCloser{Writer: ioutil.Discard},
	}
}

func BenchmarkGauge(b *testing.B) {
	c := NewNopClient()
	for i := 0; i < b.N; i++ {
		c.Gauge("test.gauge", 42.42, nil, 1.)
	}
}

func BenchmarkCount(b *testing.B) {
	c := NewNopClient()
	for i := 0; i < b.N; i++ {
		c.Count("test.count", 42, nil, 1.)
	}
}

func BenchmarkHistogram(b *testing.B) {
	c := NewNopClient()
	for i := 0; i < b.N; i++ {
		c.Histogram("test.histogram", 42.42, nil, 1.)
	}
}

func BenchmarkSet(b *testing.B) {
	c := NewNopClient()
	for i := 0; i < b.N; i++ {
		c.Set("test.set", "42.42", nil, 1.)
	}
}

func BenchmarkEvent(b *testing.B) {
	c := NewNopClient()
	for i := 0; i < b.N; i++ {
		c.Event(&Event{Title: "title1", Text: "text1", Priority: Normal, AlertType: Success, Tags: []string{"tagg"}})
	}
}

func BenchmarkSend(b *testing.B) {
	c := NewNopClient()
	for i := 0; i < b.N; i++ {
		c.send("test.send", "value", nil, 1)
	}
}

func BenchmarkBufferedClient(b *testing.B) {
	c := NewNopClient()
	c.bufferLength = 10000
	c.commands = make([]string, 0, c.bufferLength)
	c.flushTime = time.Millisecond * 100
	go c.watch()
	for i := 0; i < b.N; i++ {
		c.Count("test.count", 42, nil, 1.)
	}
}
