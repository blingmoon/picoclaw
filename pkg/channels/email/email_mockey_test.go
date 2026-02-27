//go:build mockey

package email

// Tests in this file use github.com/bytedance/mockey and require -gcflags="all=-N -l" to run.
// Run with: go test -tags=mockey -gcflags="all=-N -l" ./pkg/channels/email/...
// Without -tags=mockey they are not compiled; without -gcflags they may fail due to Mockey.

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/stretchr/testify/assert"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

func TestEmailChannel_checkNewEmails(t *testing.T) {
	if !strings.HasPrefix(runtime.Version(), "go1.25") {
		t.Skip("skipping test in non-go1.25.xx environment")
		return
	}

	mockey.PatchConvey("checkNewEmails", t, func() {
		msgBus := bus.NewMessageBus()
		c, err := NewEmailChannel(config.EmailConfig{
			Enabled:   true,
			AllowFrom: config.FlexibleStringSlice{},
		}, msgBus)
		if err != nil {
			t.Fatal(err)
		}
		c.lastUID = 20
		mockClient := &imapclient.Client{}
		c.imapClient = mockClient

		mimeBytes := []byte(
			"From: a@b.com\r\nTo: c@d.com\r\nSubject: Test\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nHello world",
		)
		bodySection := &imap.FetchItemBodySection{}
		fakeBuf := &imapclient.FetchMessageBuffer{
			SeqNum:   1,
			UID:      21,
			Envelope: &imap.Envelope{Subject: "Test"},
			BodySection: []imapclient.FetchBodySectionBuffer{
				{Section: bodySection, Bytes: mimeBytes},
			},
		}

		mockey.Mock(mockey.GetMethod(mockClient, "State")).To(func(*imapclient.Client) imap.ConnState {
			return imap.ConnStateSelected
		}).Build()
		mockey.Mock(mockey.GetMethod(mockClient, "UIDSearch")).
			To(func(*imapclient.Client, *imap.SearchCriteria, *imap.SearchOptions) *imapclient.SearchCommand {
				cmd := &imapclient.SearchCommand{}
				mockey.Mock(mockey.GetMethod(cmd, "Wait")).To(func(*imapclient.SearchCommand) (*imap.SearchData, error) {
					data := &imap.SearchData{}
					data.All = imap.UIDSetNum(21)
					return data, nil
				}).Build()
				return cmd
			}).Build()
		// Mock Fetch to return a non-nil command; mock Collect/Close on FetchCommand type
		var fetchCmd imapclient.FetchCommand
		mockey.Mock(mockey.GetMethod(mockClient, "Fetch")).
			To(func(*imapclient.Client, imap.NumSet, *imap.FetchOptions) *imapclient.FetchCommand {
				return &fetchCmd
			}).Build()
		mockey.Mock(mockey.GetMethod(&fetchCmd, "Collect")).
			To(func(*imapclient.FetchCommand) ([]*imapclient.FetchMessageBuffer, error) {
				return []*imapclient.FetchMessageBuffer{fakeBuf}, nil
			}).Build()
		mockey.Mock(mockey.GetMethod(&fetchCmd, "Close")).
			To(func(*imapclient.FetchCommand) error {
				return nil
			}).Build()
		mockey.Mock(mockey.GetMethod(mockClient, "Store")).
			To(func(*imapclient.Client, imap.NumSet, *imap.StoreFlags, *imap.StoreOptions) *imapclient.FetchCommand {
				return nil
			}).Build()

		ctx := context.Background()
		c.checkNewEmails(ctx)
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		msg, ok := msgBus.ConsumeInbound(timeoutCtx)
		assert.True(t, ok)
		assert.True(t, strings.Contains(msg.Content, "Hello world"))
	})
}

func TestEmailChannel_runIdleLoop(t *testing.T) {
	if !strings.HasPrefix(runtime.Version(), "go1.25") {
		t.Skip("skipping test in non-go1.25.xx environment")
		return
	}

	mockey.PatchConvey("runIdleLoop", t, func() {
		hasCheckEmail := false
		msgBus := bus.NewMessageBus()
		c, err := NewEmailChannel(config.EmailConfig{
			Enabled:   true,
			AllowFrom: config.FlexibleStringSlice{},
		}, msgBus)
		if err != nil {
			t.Fatal(err)
		}
		c.lastUID = 20
		c.idleUpdatesCh = make(chan struct{}, 1)
		mockClient := &imapclient.Client{}
		c.imapClient = mockClient

		var searchCmd imapclient.SearchCommand
		var idleCmd imapclient.IdleCommand
		mockey.Mock(mockey.GetMethod(mockClient, "State")).To(func(*imapclient.Client) imap.ConnState {
			return imap.ConnStateSelected
		}).Build()
		mockey.Mock(mockey.GetMethod(mockClient, "UIDSearch")).
			To(func(*imapclient.Client, *imap.SearchCriteria, *imap.SearchOptions) *imapclient.SearchCommand {
				return &searchCmd
			}).Build()
		mockey.Mock(mockey.GetMethod(&searchCmd, "Wait")).To(func(*imapclient.SearchCommand) (*imap.SearchData, error) {
			hasCheckEmail = true
			return &imap.SearchData{}, nil
		}).Build()
		idleDone := make(chan struct{})
		mockey.Mock(mockey.GetMethod(mockClient, "Idle")).
			To(func(*imapclient.Client) (*imapclient.IdleCommand, error) {
				return &idleCmd, nil
			}).Build()
		mockey.Mock(mockey.GetMethod(&idleCmd, "Wait")).To(func(*imapclient.IdleCommand) error {
			<-idleDone
			return nil
		}).Build()
		mockey.Mock(mockey.GetMethod(&idleCmd, "Close")).To(func(*imapclient.IdleCommand) error {
			close(idleDone)
			return nil
		}).Build()

		ctx := context.Background()
		go c.runIdleLoop(ctx)
		assert.False(t, hasCheckEmail)
		select {
		case c.idleUpdatesCh <- struct{}{}:
		default:
		}
		time.Sleep(time.Second)
		assert.True(t, hasCheckEmail)
	})
}

func TestEmailChannel_lifecycleCheck(t *testing.T) {
	if !strings.HasPrefix(runtime.Version(), "go1.25") {
		t.Skip("skipping test in non-go1.25.xx environment")
		return
	}

	mockey.PatchConvey("lifecycle test", t, func() {
		msgBus := bus.NewMessageBus()
		c, err := NewEmailChannel(config.EmailConfig{
			Enabled:       true,
			CheckInterval: 1,
			ForcedPolling: true,
			IMAPServer:    "imap.example.com",
			Username:      "testuser",
			Password:      "testpassword",
		}, msgBus)
		if err != nil {
			t.Fatal(err)
		}
		mockey.Mock(mockey.GetMethod(c, "connect")).To(func(*EmailChannel) error {
			return nil
		}).Build()
		c.imapClient = &imapclient.Client{}
		var searchCmd imapclient.SearchCommand
		var logoutCmd imapclient.Command
		mockey.Mock(mockey.GetMethod(c.imapClient, "State")).To(func(*imapclient.Client) imap.ConnState {
			return imap.ConnStateSelected
		}).Build()
		mockey.Mock(mockey.GetMethod(c.imapClient, "UIDSearch")).
			To(func(*imapclient.Client, *imap.SearchCriteria, *imap.SearchOptions) *imapclient.SearchCommand {
				return &searchCmd
			}).Build()
		mockey.Mock(mockey.GetMethod(&searchCmd, "Wait")).To(func(*imapclient.SearchCommand) (*imap.SearchData, error) {
			time.Sleep(1 * time.Second)
			return &imap.SearchData{}, nil
		}).Build()
		mockey.Mock(mockey.GetMethod(c.imapClient, "Logout")).
			To(func(*imapclient.Client) *imapclient.Command {
				return &logoutCmd
			}).Build()
		mockey.Mock(mockey.GetMethod(&logoutCmd, "Wait")).To(func(*imapclient.Command) error {
			return nil
		}).Build()

		ctx := context.Background()
		err = c.Start(ctx)
		assert.NoError(t, err)
		wg := sync.WaitGroup{}
		wg.Add(1)
		var stopDone time.Time
		stopStart := time.Now()
		go func() {
			defer wg.Done()
			c.Stop(ctx)
			stopDone = time.Now()
		}()
		assert.True(t, c.IsRunning())
		wg.Wait()
		elapsed := stopDone.Sub(stopStart)
		assert.False(t, c.IsRunning())
		assert.GreaterOrEqual(t, elapsed, 1*time.Second, "Stop() must wait for checkLoop (lifecycle compliance)")
	})
}
