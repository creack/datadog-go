// Copyright 2013 Ooyala, Inc.

/*
Package statsd provides a Go dogstatsd client. Dogstatsd extends the popular statsd,
adding tags and histograms and pushing upstream to Datadog.

Refer to http://docs.datadoghq.com/guides/dogstatsd/ for information about DogStatsD.

Example Usage:

	// Create the client
	c, err := statsd.New("127.0.0.1:8125")
	if err != nil {
		log.Fatal(err)
	}
	// Prefix every metric with the app name
	c.Namespace = "flubber."
	// Send the EC2 availability zone as a tag with every metric
	c.Tags = append(c.Tags, "us-east-1a")
	err = c.Gauge("request.duration", 1.2, nil, 1)

statsd is based on go-statsd-client.
*/
package statsd

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Common errors.
var (
	ErrClientNotInitialized = errors.New("the client is not initialized")
	ErrMissingEventTitle    = errors.New("statsd.Event title is required")
	ErrMissingEventText     = errors.New("statsd.Event text is required")
)

// A Client is a handle for sending udp messages to dogstatsd.  It is safe to
// use one Client from multiple goroutines simultaneously.
type Client struct {
	sync.RWMutex

	conn         io.WriteCloser
	Namespace    string   // Namespace to prepend to all statsd calls
	Tags         []string // Tags are global tags to be added to every statsd call
	bufferLength int      // bufferLength is the length of the buffer in commands.
	flushTime    time.Duration
	commands     []string
	stopChan     chan struct{}
}

// New returns a pointer to a new Client given an addr in the format "hostname:port".
func New(addr string) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	client := &Client{
		conn:     conn,
		stopChan: make(chan struct{}),
	}
	return client, nil
}

// NewBuffered returns a Client that buffers its output and sends it in chunks.
// Buflen is the length of the buffer in number of commands.
func NewBuffered(addr string, buflen int) (*Client, error) {
	client, err := New(addr)
	if err != nil {
		return nil, err
	}
	client.bufferLength = buflen
	client.commands = make([]string, 0, buflen)
	client.flushTime = 100 * time.Millisecond
	go client.watch()
	return client, nil
}

// format a message from its name, value, tags and rate.  Also adds global
// namespace and tags.
func (c *Client) format(name, value string, tags []string, rate float64) string {
	var buf string
	if c.Namespace != "" {
		buf = c.Namespace
	}
	buf += name + ":" + value
	if rate < 1 {
		buf += "|@" + strconv.FormatFloat(rate, 'f', -1, 64)
	}
	if len(c.Tags)+len(tags) > 0 {
		buf += "|#" + strings.Join(append(c.Tags, tags...), ",")
	}
	return buf
}

func (c *Client) watch() {
	ticker := time.NewTicker(c.flushTime)
	for {
		select {
		case <-c.stopChan:
			ticker.Stop()
			return
		case <-ticker.C:
			c.Lock()
			if len(c.commands) > 0 {
				// FIXME: eating error here
				c.flush()
			}
			c.Unlock()
		}
	}
}

func (c *Client) aggregate(cmd string) error {
	c.Lock()
	c.commands = append(c.commands, cmd)
	// if we should flush, lets do it
	if len(c.commands) == c.bufferLength {
		if err := c.flush(); err != nil {
			c.Unlock()
			return err
		}
	}
	c.Unlock()
	return nil
}

// flush the commands in the buffer.  Lock must be held by caller.
func (c *Client) flush() error {
	data := strings.Join(c.commands, "\n")
	_, err := c.conn.Write([]byte(data))
	c.commands = make([]string, 0, c.bufferLength)
	return err
}

func (c *Client) sendMsg(msg string) error {
	// if this client is buffered, then we'll just append this
	if c.bufferLength > 0 {
		return c.aggregate(msg)
	}
	_, err := c.conn.Write([]byte(msg))
	return err
}

// send handles sampling and sends the message over UDP. It also adds global namespace prefixes and tags.
func (c *Client) send(name, value string, tags []string, rate float64) error {
	if c == nil {
		return ErrClientNotInitialized
	}
	if rate < 1 && rand.Float64() > rate {
		return nil
	}
	return c.sendMsg(c.format(name, value, tags, rate))
}

// Gauge measures the value of a metric at a particular time.
func (c *Client) Gauge(name string, value float64, tags []string, rate float64) error {
	return c.send(name, strconv.FormatFloat(value, 'f', 3, 64)+"|g", tags, rate)
}

// Count tracks how many times something happened per second.
func (c *Client) Count(name string, value int64, tags []string, rate float64) error {
	return c.send(name, strconv.FormatInt(value, 10)+"|c", tags, rate)
}

// Histogram tracks the statistical distribution of a set of values.
func (c *Client) Histogram(name string, value float64, tags []string, rate float64) error {
	return c.send(name, strconv.FormatFloat(value, 'f', 3, 64)+"|h", tags, rate)
}

// Set counts the number of unique elements in a group.
func (c *Client) Set(name, value string, tags []string, rate float64) error {
	return c.send(name, value+"|s", tags, rate)
}

// Event sends the provided Event.
func (c *Client) Event(e *Event) error {
	stat, err := e.Encode(c.Tags...)
	if err != nil {
		return err
	}
	return c.sendMsg(stat)
}

// SimpleEvent sends an event with the provided title and text.
func (c *Client) SimpleEvent(title, text string) error {
	return c.Event(NewEvent(title, text))
}

// Close the client connection.
func (c *Client) Close() error {
	if c == nil {
		return ErrClientNotInitialized
	}
	close(c.stopChan)
	return c.conn.Close()
}

// Events support

type eventAlertType string

// Event type enum
const (
	Info    eventAlertType = "info"    // Info is the "info" AlertType for events
	Error   eventAlertType = "error"   // Error is the "error" AlertType for events
	Warning eventAlertType = "warning" // Warning is the "warning" AlertType for events
	Success eventAlertType = "success" // Success is the "success" AlertType for events
)

type eventPriority string

// Event priority enum
const (
	Normal eventPriority = "normal" // Normal is the "normal" Priority for events
	Low    eventPriority = "low"    // Low is the "low" Priority for events
)

// An Event is an object that can be posted to your DataDog event stream.
type Event struct {
	Title          string         // Title of the event.  Required.
	Text           string         // Text is the description of the event.  Required.
	Timestamp      time.Time      // Timestamp is a timestamp for the event.  If not provided, the dogstatsd server will set this to the current time.
	Hostname       string         // Hostname for the event.
	AggregationKey string         // AggregationKey groups this event with others of the same key.
	Priority       eventPriority  // Priority of the event.  Can be statsd.Low or statsd.Normal.
	SourceTypeName string         // SourceTypeName is a source type for the event.
	AlertType      eventAlertType // AlertType can be statsd.Info, statsd.Error, statsd.Warning, or statsd.Success. If absent, the default value applied by the dogstatsd server is Info.
	Tags           []string       // Tags for the event.
}

// NewEvent creates a new event with the given title and text.  Error checking
// against these values is done at send-time, or upon running e.Check.
func NewEvent(title, text string) *Event {
	return &Event{
		Title: title,
		Text:  text,
	}
}

// Check verifies that an event is valid.
func (e Event) Check() error {
	if len(e.Title) == 0 {
		return ErrMissingEventTitle
	}
	if len(e.Text) == 0 {
		return ErrMissingEventText
	}
	return nil
}

// Encode returns the dogstatsd wire protocol representation for an event.
// Tags may be passed which will be added to the encoded output but not to
// the Event's list of tags, eg. for default tags.
func (e Event) Encode(tags ...string) (string, error) {
	if err := e.Check(); err != nil {
		return "", err
	}
	buf := "_e{" + strconv.FormatInt(int64(len(e.Title)), 10) + "," + strconv.FormatInt(int64(len(e.Text)), 10) + "}:" + e.Title + "|" + e.Text

	if !e.Timestamp.IsZero() {
		buf += "|d:" + strconv.FormatInt(int64(e.Timestamp.Unix()), 10)
	}

	if len(e.Hostname) != 0 {
		buf += "|h:" + e.Hostname
	}

	if len(e.AggregationKey) != 0 {
		buf += "|k:" + e.AggregationKey

	}

	if len(e.Priority) != 0 {
		buf += "|p:" + string(e.Priority)
	}

	if len(e.SourceTypeName) != 0 {
		buf += "|s:" + e.SourceTypeName
	}

	if len(e.AlertType) != 0 {
		buf += "|t:" + string(e.AlertType)
	}

	if len(tags)+len(e.Tags) > 0 {
		buf += "|#" + strings.Join(append(tags, e.Tags...), ",")
	}
	return buf, nil
}
