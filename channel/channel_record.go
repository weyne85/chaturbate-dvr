package channel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/teacat/chaturbate-dvr/chaturbate"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
)

// Monitor starts monitoring the channel for live streams and records them.
func (ch *Channel) Monitor() {
	client := chaturbate.NewClient()
	ch.Info("starting to record `%s`", ch.Config.Username)

	// Create a new context with a cancel function,
	// the CancelFunc will be stored in the channel's CancelFunc field
	// and will be called by `Pause` or `Stop` functions
	ctx, _ := ch.WithCancel(context.Background())

	var err error
	for {
		if err = ctx.Err(); err != nil {
			break
		}

		pipeline := func() error {
			return ch.RecordStream(ctx, client)
		}
		onRetry := func(_ uint, err error) {
			if errors.Is(err, internal.ErrChannelOffline) {
				ch.Info("channel is offline, try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, internal.ErrCloudflareBlocked) {
				ch.Info("channel was blocked by Cloudflare; try with `-cookies` and `-user-agent`? try again in %d min(s)", server.Config.Interval)
			} else if errors.Is(err, context.Canceled) {
				// ...
			} else {
				ch.Error("on retry: %s: retrying in %d min(s)", err.Error(), server.Config.Interval)
			}
		}
		if err = retry.Do(
			pipeline,
			retry.Context(ctx),
			retry.Attempts(0),
			retry.Delay(time.Duration(server.Config.Interval)*time.Minute),
			retry.DelayType(retry.FixedDelay),
			retry.OnRetry(onRetry),
		); err != nil {
			break
		}
	}

	if err != nil {
		// A canceled context or an intentional pause are expected control-flow
		// outcomes, not failures, so don't log them as errors.
		if !errors.Is(err, context.Canceled) && !errors.Is(err, internal.ErrPaused) {
			ch.Error("record stream: %s", err.Error())
		}
		if err := ch.Cleanup(); err != nil {
			ch.Error("cleanup canceled channel: %s", err.Error())
		}
	}
}

// Update sends an update signal to the channel's update channel.
// This notifies the Server-sent Event to boradcast the channel information to the client.
func (ch *Channel) Update() {
	ch.UpdateCh <- true
}

// RecordStream records the stream of the channel using the provided client.
// It retrieves the stream information and starts watching the segments.
func (ch *Channel) RecordStream(ctx context.Context, client *chaturbate.Client) error {
	stream, err := client.GetStream(ctx, ch.Config.Username)
	if err != nil {
		ch.mu.Lock()
		ch.IsOnline = false
		ch.mu.Unlock()
		return fmt.Errorf("get stream: %w", err)
	}
	ch.mu.Lock()
	ch.IsOnline = true
	ch.StreamedAt = time.Now().Unix()
	ch.mu.Unlock()

	if err := ch.NextFile(); err != nil {
		return fmt.Errorf("next file: %w", err)
	}

	playlist, err := stream.GetPlaylist(ctx, ch.Config.Resolution, ch.Config.Framerate)
	if err != nil {
		return fmt.Errorf("get playlist: %w", err)
	}
	ch.Info("stream quality - resolution %dp (target: %dp), framerate %dfps (target: %dfps)", playlist.Resolution, ch.Config.Resolution, playlist.Framerate, ch.Config.Framerate)

	return playlist.WatchSegments(ctx, ch.HandleSegment)
}

// HandleSegment processes and writes segment data to a file.
func (ch *Channel) HandleSegment(b []byte, duration float64) error {
	ch.mu.RLock()
	isPaused := ch.Config.IsPaused
	file := ch.File
	ch.mu.RUnlock()

	if isPaused {
		return retry.Unrecoverable(internal.ErrPaused)
	}

	n, err := file.Write(b)
	if err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	ch.mu.Lock()
	ch.Filesize += n
	ch.Duration += duration
	filesize, dur := ch.Filesize, ch.Duration
	ch.mu.Unlock()
	ch.Info("duration: %s, filesize: %s", internal.FormatDuration(dur), internal.FormatFilesize(filesize))

	// Send an SSE update to update the view
	ch.Update()

	if ch.ShouldSwitchFile() {
		if err := ch.NextFile(); err != nil {
			return fmt.Errorf("next file: %w", err)
		}
		ch.mu.RLock()
		newFilename := ch.File.Name()
		ch.mu.RUnlock()
		ch.Info("max filesize or duration exceeded, new file created: %s", newFilename)
		return nil
	}
	return nil
}
