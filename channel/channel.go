package channel

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
)

// Channel represents a channel instance.
type Channel struct {
	CancelFunc context.CancelFunc
	LogCh      chan string
	UpdateCh   chan bool
	done       chan struct{}
	stopOnce   sync.Once

	// mu guards every field below that is read or written from more than
	// one goroutine (the monitor loop, the publisher loop, and HTTP handlers).
	mu sync.RWMutex

	IsOnline   bool
	StreamedAt int64
	Duration   float64 // Seconds
	Filesize   int     // Bytes
	Sequence   int

	Logs []string

	File   *os.File
	Config *entity.ChannelConfig
}

// New creates a new channel instance with the given manager and configuration.
func New(conf *entity.ChannelConfig) *Channel {
	ch := &Channel{
		LogCh:      make(chan string),
		UpdateCh:   make(chan bool),
		done:       make(chan struct{}),
		Config:     conf,
		CancelFunc: func() {},
	}
	go ch.Publisher()

	return ch
}

// Publisher listens for log messages and updates from the channel
// and publishes once received. It exits once the channel is stopped.
func (ch *Channel) Publisher() {
	for {
		select {
		case v := <-ch.LogCh:
			// Append the log message to ch.Logs and keep only the last 100 rows
			ch.mu.Lock()
			ch.Logs = append(ch.Logs, v)
			if len(ch.Logs) > 100 {
				ch.Logs = ch.Logs[len(ch.Logs)-100:]
			}
			ch.mu.Unlock()
			server.Manager.Publish(entity.EventLog, ch.ExportInfo())

		case <-ch.UpdateCh:
			server.Manager.Publish(entity.EventUpdate, ch.ExportInfo())

		case <-ch.done:
			return
		}
	}
}

// WithCancel creates a new context with a cancel function,
// then stores the cancel function in the channel's CancelFunc field.
//
// This is used to cancel the context when the channel is stopped or paused.
func (ch *Channel) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, ch.CancelFunc = context.WithCancel(ctx)
	return ctx, ch.CancelFunc
}

// Info logs an informational message.
func (ch *Channel) Info(format string, a ...any) {
	ch.LogCh <- fmt.Sprintf("%s [INFO] %s", time.Now().Format("15:04"), fmt.Sprintf(format, a...))
	log.Printf(" INFO [%s] %s", ch.Config.Username, fmt.Sprintf(format, a...))
}

// Error logs an error message.
func (ch *Channel) Error(format string, a ...any) {
	ch.LogCh <- fmt.Sprintf("%s [ERROR] %s", time.Now().Format("15:04"), fmt.Sprintf(format, a...))
	log.Printf("ERROR [%s] %s", ch.Config.Username, fmt.Sprintf(format, a...))
}

// ExportInfo exports the channel information as a ChannelInfo struct.
func (ch *Channel) ExportInfo() *entity.ChannelInfo {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	var filename string
	if ch.File != nil {
		filename = ch.File.Name()
	}
	var streamedAt string
	if ch.StreamedAt != 0 {
		streamedAt = time.Unix(ch.StreamedAt, 0).Format("2006-01-02 15:04 AM")
	}
	// Copy the logs slice so callers don't share backing storage with ch.Logs.
	logs := make([]string, len(ch.Logs))
	copy(logs, ch.Logs)

	return &entity.ChannelInfo{
		IsOnline:     ch.IsOnline,
		IsPaused:     ch.Config.IsPaused,
		Username:     ch.Config.Username,
		MaxDuration:  internal.FormatDuration(float64(ch.Config.MaxDuration * 60)), // MaxDuration from config is in minutes
		MaxFilesize:  internal.FormatFilesize(ch.Config.MaxFilesize * 1024 * 1024), // MaxFilesize from config is in MB
		StreamedAt:   streamedAt,
		CreatedAt:    ch.Config.CreatedAt,
		Duration:     internal.FormatDuration(ch.Duration),
		Filesize:     internal.FormatFilesize(ch.Filesize),
		Filename:     filename,
		Logs:         logs,
		GlobalConfig: server.Config,
	}
}

// ExportConfig returns a snapshot copy of the channel's configuration,
// safe to read concurrently (e.g. for JSON persistence).
func (ch *Channel) ExportConfig() *entity.ChannelConfig {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	confCopy := *ch.Config
	return &confCopy
}

// Pause pauses the channel and cancels the context.
func (ch *Channel) Pause() {
	// Stop the monitoring loop
	ch.CancelFunc()

	ch.mu.Lock()
	ch.Config.IsPaused = true
	ch.Sequence = 0
	ch.IsOnline = false
	ch.mu.Unlock()

	ch.Update()
	ch.Info("channel paused")
}

// Stop stops the channel, cancels the context, and shuts down its
// background publisher goroutine. Safe to call more than once.
func (ch *Channel) Stop() {
	// Stop the monitoring loop
	ch.CancelFunc()

	ch.Info("channel stopped")

	// Shut down the publisher goroutine now that no more updates will follow.
	ch.stopOnce.Do(func() {
		close(ch.done)
	})
}

// Resume resumes the channel monitoring.
//
// `startSeq` is used to prevent all channels from starting at the same time, preventing TooManyRequests errors.
// It's only be used when program starting and trying to resume all channels at once.
func (ch *Channel) Resume(startSeq int) {
	ch.mu.Lock()
	ch.Config.IsPaused = false
	ch.mu.Unlock()

	ch.Update()
	ch.Info("channel resumed")

	<-time.After(time.Duration(startSeq) * time.Second)
	ch.Monitor()
}
