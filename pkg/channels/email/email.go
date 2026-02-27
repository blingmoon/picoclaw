package email

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"mime"
	"net"
	"net/smtp"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	charset "github.com/emersion/go-message/charset"
	"github.com/emersion/go-message/mail"
	"github.com/google/uuid"
	"golang.org/x/text/encoding/simplifiedchinese"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/identity"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/utils"
)

func init() {
	// Register GBK so go-message can decode mail body (e.g. QQ/163 mailboxes); otherwise "unhandled charset \"gbk\"".
	charset.RegisterEncoding("gbk", simplifiedchinese.GBK)
}

const (
	// reconnect backoff initial
	reconnectBackoffInitial = 1 * time.Second
	// reconnect backoff max
	reconnectBackoffMax = 10 * time.Minute
	// default attachment max bytes
	defaultAttachmentMaxBytes = 25 * 1024 * 1024 // 25MB
	// max bytes to read per body part (text/plain, text/html) to avoid unbounded io.ReadAll
	defaultBodyPartMaxBytes = 1 * 1024 * 1024 // 1MB
)

type EmailChannel struct {
	*channels.BaseChannel
	config        config.EmailConfig
	imapClient    *imapclient.Client
	lastUID       uint32
	idleUpdatesCh chan struct{} // used by IDLE unilateral handler to signal new mail
	// checkEmailMutex
	// maybe multiple goroutine check email at the same time, use mutex to avoid duplicate check
	checkEmailMutex sync.Mutex

	mu          sync.Mutex
	cancel      context.CancelFunc
	checkTicker *time.Ticker

	// loopWg waits for checkLoop goroutine to exit in Stop().
	loopWg sync.WaitGroup

	// reconnect control
	//
	// reconnectClientVersion is an atomic counter incremented on every successful reconnect.
	// Before acquiring reconnectMutex a goroutine snapshots the counter; after acquiring
	// the lock it re-reads and compares: if the value is unchanged the goroutine is the
	// first to hold the lock since the connection broke, so it performs the reconnect;
	// if the value has changed another goroutine already reconnected, so it exits early.
	// Using atomic.Int64 makes the pre-lock Load() race-free.
	reconnectClientVersion atomic.Int64
	reconnectMutex         sync.Mutex
}

func NewEmailChannel(cfg config.EmailConfig, bus *bus.MessageBus) (*EmailChannel, error) {
	base := channels.NewBaseChannel("email", cfg, bus, cfg.AllowFrom)
	return &EmailChannel{
		BaseChannel: base,
		config:      cfg,
		lastUID:     0,
	}, nil
}

func (c *EmailChannel) Start(ctx context.Context) error {
	if !c.config.Enabled {
		return fmt.Errorf("email channel is not enabled")
	}
	if c.config.IMAPServer == "" || c.config.Username == "" || c.config.Password == "" {
		return fmt.Errorf("email IMAP server, username or password is empty")
	}

	logger.InfoC("email", "Starting Email channel")

	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()

	// Channel for IDLE unilateral updates (new mail); connect() will set UnilateralDataHandler to send here.
	c.idleUpdatesCh = make(chan struct{}, 1)
	if err := c.connect(); err != nil {
		cancel()
		return fmt.Errorf("failed to connect to IMAP server: %w", err)
	}

	c.SetRunning(true)
	logger.InfoC("email", "Email channel started")

	c.loopWg.Add(1)
	go c.checkLoop(runCtx)

	return nil
}

func (c *EmailChannel) Stop(ctx context.Context) error {
	logger.InfoC("email", "Stopping Email channel")

	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	if c.checkTicker != nil {
		c.checkTicker.Stop()
		c.checkTicker = nil
	}
	imapClient := c.imapClient
	c.imapClient = nil
	c.mu.Unlock()
	if imapClient != nil {
		_ = imapClient.Logout().Wait()
	}

	c.loopWg.Wait() // wait for checkLoop goroutine to exit

	c.SetRunning(false)
	logger.InfoC("email", "Email channel stopped")
	return nil
}

// sanitizeHeaderValue removes CR/LF from s to prevent SMTP header injection.
// go-message textproto also rejects \r\n in header values when writing; we sanitize so the send succeeds.
func sanitizeHeaderValue(s string) string {
	return strings.NewReplacer("\r", "", "\n", "").Replace(s)
}

func (c *EmailChannel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	if !c.IsRunning() {
		return channels.ErrNotRunning
	}
	if strings.TrimSpace(c.config.SMTPServer) == "" {
		return fmt.Errorf("email channel send: SMTP not configured (set smtp_server)")
	}

	fromRaw := sanitizeHeaderValue(c.config.Username)
	toRaw := sanitizeHeaderValue(strings.TrimSpace(msg.ChatID))
	if toRaw == "" {
		return fmt.Errorf("email channel send: missing recipient (chat_id)")
	}

	// Build message with go-message/mail: RFC-compliant headers via textproto (folding, encoded-words, address list format).
	var h mail.Header
	if fromAddrs, err := mail.ParseAddressList(fromRaw); err == nil && len(fromAddrs) > 0 {
		h.SetAddressList("From", fromAddrs)
	} else {
		h.Set("From", fromRaw)
	}
	if toAddrs, err := mail.ParseAddressList(toRaw); err == nil && len(toAddrs) > 0 {
		h.SetAddressList("To", toAddrs)
	} else {
		h.Set("To", toRaw)
	}
	h.SetSubject(sanitizeHeaderValue("Reply from PicoClaw"))
	h.Set("Content-Type", "text/plain; charset=utf-8")
	var buf bytes.Buffer
	bodyWriter, err := mail.CreateSingleInlineWriter(&buf, h)
	if err != nil {
		return fmt.Errorf("email build message: %w", err)
	}
	if _, err = bodyWriter.Write([]byte(msg.Content)); err != nil {
		_ = bodyWriter.Close()
		return fmt.Errorf("email write body: %w", err)
	}
	if err = bodyWriter.Close(); err != nil {
		return fmt.Errorf("email close message: %w", err)
	}
	body := buf.Bytes()

	port := c.config.SMTPPort
	if port <= 0 {
		port = 465
	}
	addr := net.JoinHostPort(c.config.SMTPServer, strconv.Itoa(port))
	host := c.config.SMTPServer

	if c.config.SMTPUseTLS {
		// Port 465: implicit TLS
		tlsConfig := &tls.Config{ServerName: host}
		conn, tlserr := tls.Dial("tcp", addr, tlsConfig)
		if tlserr != nil {
			return fmt.Errorf("smtp tls dial: %w", tlserr)
		}
		defer conn.Close()
		client, newClientErr := smtp.NewClient(conn, host)
		if newClientErr != nil {
			return fmt.Errorf("smtp new client: %w", newClientErr)
		}
		defer client.Close()
		auth := smtp.PlainAuth("", c.config.Username, c.config.Password, host)
		if err = client.Auth(auth); err != nil {
			return fmt.Errorf("smtp auth: %w", err)
		}
		if err = client.Mail(fromRaw); err != nil {
			return fmt.Errorf("smtp mail: %w", err)
		}
		if err = client.Rcpt(toRaw); err != nil {
			return fmt.Errorf("smtp rcpt: %w", err)
		}
		w, dataErr := client.Data()
		if dataErr != nil {
			return fmt.Errorf("smtp data: %w", dataErr)
		}
		if _, err = w.Write(body); err != nil {
			_ = w.Close()
			return fmt.Errorf("smtp write: %w", err)
		}
		if err = w.Close(); err != nil {
			return fmt.Errorf("smtp data close: %w", err)
		}
		return client.Quit()
	}

	// Port 587 etc.: TCP first, then STARTTLS if needed
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("smtp dial: %w", err)
	}
	defer conn.Close()
	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return fmt.Errorf("smtp new client: %w", err)
	}
	defer client.Close()
	if err = client.StartTLS(&tls.Config{ServerName: host}); err != nil {
		// Some servers on 587 do not require STARTTLS; continue anyway
		logger.WarnCF("email",
			"STARTTLS failed, connection may be unencrypted; credentials could be sent in plaintext",
			map[string]any{"error": err.Error()})
		_ = err
	}
	auth := smtp.PlainAuth("", c.config.Username, c.config.Password, host)
	if err = client.Auth(auth); err != nil {
		return fmt.Errorf("smtp auth: %w", err)
	}
	if err = client.Mail(fromRaw); err != nil {
		return fmt.Errorf("smtp mail: %w", err)
	}
	if err = client.Rcpt(toRaw); err != nil {
		return fmt.Errorf("smtp rcpt: %w", err)
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp data: %w", err)
	}
	if _, err = w.Write(body); err != nil {
		_ = w.Close()
		return fmt.Errorf("smtp write: %w", err)
	}
	if err = w.Close(); err != nil {
		return fmt.Errorf("smtp data close: %w", err)
	}
	return client.Quit()
}

func (c *EmailChannel) connect() error {
	address := fmt.Sprintf("%s:%d", c.config.IMAPServer, c.config.IMAPPort)

	opts := &imapclient.Options{
		WordDecoder: &mime.WordDecoder{CharsetReader: charset.Reader},
		UnilateralDataHandler: &imapclient.UnilateralDataHandler{
			Mailbox: func(data *imapclient.UnilateralDataMailbox) {
				if data.NumMessages != nil && c.idleUpdatesCh != nil {
					select {
					case c.idleUpdatesCh <- struct{}{}:
					default:
					}
				}
			},
		},
	}

	var cl *imapclient.Client
	var err error
	if c.config.UseTLS {
		cl, err = imapclient.DialTLS(address, opts)
	} else {
		cl, err = imapclient.DialInsecure(address, opts)
	}
	if err != nil {
		return err
	}

	if loginErr := cl.Login(c.config.Username, c.config.Password).Wait(); loginErr != nil {
		_ = cl.Logout().Wait()
		return loginErr
	}

	c.mu.Lock()
	c.imapClient = cl
	c.mu.Unlock()

	mailbox := c.config.Mailbox
	if mailbox == "" {
		mailbox = "INBOX"
	}
	if c.checkIsWangYiEmail() {
		_, idErr := cl.ID(&imap.IDData{Name: "picoclaw", Version: "1.0"}).Wait()
		if idErr != nil {
			return fmt.Errorf("failed to execute ID COMMAND: %w", idErr)
		}
	}
	selectCmd := cl.Select(mailbox, &imap.SelectOptions{ReadOnly: false})
	selectData, err := selectCmd.Wait()
	if err != nil {
		return fmt.Errorf("failed to select mailbox %s: %w", mailbox, err)
	}

	if selectData != nil && selectData.UIDNext > 0 {
		c.mu.Lock()
		if c.lastUID == 0 {
			c.lastUID = uint32(selectData.UIDNext) - 1
		}
		c.mu.Unlock()
	} else {
		if err := c.syncLastUID(cl); err != nil {
			_ = cl.Logout().Wait()
			return fmt.Errorf("failed to sync mailbox UID: %w", err)
		}
	}

	logger.InfoCF("email", "Connected to IMAP server", map[string]any{
		"server":   c.config.IMAPServer,
		"mailbox":  mailbox,
		"last_uid": c.lastUID,
	})
	return nil
}

func (c *EmailChannel) checkIsWangYiEmail() bool {
	server := strings.ToLower(c.config.IMAPServer)
	return strings.Contains(server, "163.com") ||
		strings.Contains(server, "126.com") ||
		strings.Contains(server, "yeah.net")
}

// syncLastUID fetches the mailbox max UID and sets lastUID so only mail after connect is processed.
func (c *EmailChannel) syncLastUID(cl *imapclient.Client) error {
	c.mu.Lock()
	if c.lastUID != 0 {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	criteria := &imap.SearchCriteria{}
	data, err := cl.UIDSearch(criteria, nil).Wait()
	if err != nil {
		var all imap.UIDSet
		all.AddRange(imap.UID(1), 0)
		criteria.UID = []imap.UIDSet{all}
		data, err = cl.UIDSearch(criteria, nil).Wait()
		if err != nil {
			return err
		}
	}
	uids := data.AllUIDs()
	var maxUID uint32
	for _, uid := range uids {
		if uint32(uid) > maxUID {
			maxUID = uint32(uid)
		}
	}
	c.mu.Lock()
	if c.lastUID == 0 {
		c.lastUID = maxUID
	}
	c.mu.Unlock()
	return nil
}

// closeIMAPClient logs out and clears the current IMAP client. Caller must not hold c.mu.
func (c *EmailChannel) closeIMAPClient() {
	c.mu.Lock()
	cl := c.imapClient
	c.imapClient = nil
	c.mu.Unlock()
	if cl != nil {
		_ = cl.Logout().Wait()
	}
}

// reconnectWithBackoff closes the current IMAP client and reconnects with exponential backoff until success or ctx is done.
// At most one goroutine performs the actual reconnect; the rest detect the version bump and exit early.
func (c *EmailChannel) reconnectWithBackoff(ctx context.Context) error {
	// Snapshot the version atomically BEFORE acquiring the mutex.
	// This read is always race-free because reconnectClientVersion is an atomic.Int64.
	currentClientVersion := c.reconnectClientVersion.Load()

	c.reconnectMutex.Lock()
	defer c.reconnectMutex.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Re-read the version under the mutex.
	// If it differs from our snapshot, another goroutine incremented it and already
	// performed a reconnect while we were waiting — no need to reconnect again.
	if c.reconnectClientVersion.Load() != currentClientVersion {
		c.mu.Lock()
		isOk := c.imapClient != nil && c.imapClient.State() == imap.ConnStateSelected
		c.mu.Unlock()
		if isOk {
			return nil
		}
		// Version changed but client is still broken; fall through and reconnect anyway.
	}

	c.closeIMAPClient()
	backoff := reconnectBackoffInitial
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.connect()
		if err == nil {
			// Increment only on success so goroutines still waiting on the mutex
			// can distinguish "reconnect succeeded" from "reconnect failed".
			c.reconnectClientVersion.Add(1)
			return nil
		}
		logger.ErrorCF("email", "IMAP reconnect failed, retrying with backoff", map[string]any{
			"error": err.Error(), "backoff": backoff.String(),
		})
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			if backoff < reconnectBackoffMax {
				backoff *= 2
				if backoff > reconnectBackoffMax {
					backoff = reconnectBackoffMax
				}
			}
		}
	}
}

func (c *EmailChannel) checkLoop(ctx context.Context) {
	defer c.loopWg.Done()
	interval := time.Duration(c.config.CheckInterval) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// Run one check immediately
	c.checkNewEmails(ctx)
	// support IDLE user idle loop, waiting for server push update
	isSupportIDLE := false
	c.mu.Lock()
	cl := c.imapClient
	c.mu.Unlock()
	if cl == nil {
		return
	}
	// check is support IDLE
	caps, err := c.imapClient.Capability().Wait()
	if err != nil {
		isSupportIDLE = false
		logger.ErrorCF("email", "Failed to get capabilities", map[string]any{"error": err.Error()})
	} else {
		if caps.Has(imap.CapIdle) {
			isSupportIDLE = true
		}
	}

	if !c.config.ForcedPolling {
		if isSupportIDLE {
			c.runIdleLoop(ctx)
			return
		} else {
			logger.ErrorC("email", "exception: IDLE,  but server is not supported, using polling mode")
		}
		return
	}

	// not support IDLE user polling mode, check new emails every interval
	c.mu.Lock()
	c.checkTicker = time.NewTicker(interval)
	ticker := c.checkTicker
	c.mu.Unlock()
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkNewEmails(ctx)
		}
	}
}

// runIdleLoop uses IMAP IDLE (RFC 2177). When the server pushes a mailbox update (e.g. * EXISTS for new mail),
// UnilateralDataHandler.Mailbox sends to idleUpdatesCh; we close the Idle command and run checkNewEmails().
func (c *EmailChannel) runIdleLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		c.mu.Lock()
		cl := c.imapClient
		c.mu.Unlock()
		if cl == nil {
			return
		}
		if cl.State() != imap.ConnStateSelected {
			if err := c.reconnectWithBackoff(ctx); err != nil {
				logger.ErrorCF("email", "Failed to reconnect after IDLE error", map[string]any{"error": err.Error()})
				return
			}
			continue
		}
		idleCmd, err := cl.Idle()
		if err != nil {
			logger.ErrorCF("email", "IDLE failed", map[string]any{"error": err.Error()})
			if err := c.reconnectWithBackoff(ctx); err != nil {
				return
			}
			continue
		}
		done := make(chan error, 1)
		go func() { done <- idleCmd.Wait() }()
		select {
		case <-ctx.Done():
			_ = idleCmd.Close()
			<-done
			return
		case <-c.idleUpdatesCh:
			_ = idleCmd.Close()
			if err := <-done; err != nil {
				logger.ErrorCF("email", "IDLE ended with error after update", map[string]any{"error": err.Error()})
				if err := c.reconnectWithBackoff(ctx); err != nil {
					return
				}
			}
			c.checkNewEmails(ctx)
		case err := <-done:
			if err != nil {
				logger.ErrorCF("email", "IDLE ended with error", map[string]any{"error": err.Error()})
				if err := c.reconnectWithBackoff(ctx); err != nil {
					return
				}
			}
			c.checkNewEmails(ctx)
		}
	}
}

func (c *EmailChannel) checkNewEmails(ctx context.Context) {
	// the lock is to avoid duplicate check email, maybe multiple goroutine check email at the same time
	c.checkEmailMutex.Lock()
	defer c.checkEmailMutex.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		c.mu.Lock()
		cl := c.imapClient
		lastUID := c.lastUID
		c.mu.Unlock()

		if cl == nil {
			return
		}

		// Check connection state; reconnect with backoff if needed
		if cl.State() != imap.ConnStateSelected {
			if err := c.reconnectWithBackoff(ctx); err != nil {
				return
			}
			continue
		}

		// Only process mail after recorded lastUID (search by UID range, not by unread)
		criteria := &imap.SearchCriteria{
			// NotFlag: []imap.Flag{imap.FlagSeen},
		}
		if lastUID > 0 {
			var uidSet imap.UIDSet
			uidSet.AddRange(imap.UID(lastUID+1), 0)
			criteria.UID = []imap.UIDSet{uidSet}
		}

		searchData, err := cl.UIDSearch(criteria, nil).Wait()
		if err != nil {
			logger.ErrorCF("email", "Failed to search emails", map[string]any{
				"error": err.Error(),
			})
			c.closeIMAPClient()
			if reconnectErr := c.reconnectWithBackoff(ctx); reconnectErr != nil {
				logger.ErrorCF("email", "Failed to reconnect after search emails error",
					map[string]any{"error": reconnectErr.Error()})
				return
			}
			continue
		}

		searchUids := searchData.AllUIDs()
		uids := make([]imap.UID, 0, len(searchUids))
		for _, uid := range searchUids {
			if uid > imap.UID(lastUID) {
				uids = append(uids, uid)
			}
		}
		if len(uids) == 0 {
			return
		}

		maxUID, err := c.streamFetchEmail(ctx, cl, uids)
		if err != nil {
			logger.ErrorCF("email", "Failed to stream fetch emails", map[string]any{
				"error": err.Error(),
			})
			return
		}

		// Update last processed UID
		if maxUID > 0 {
			c.mu.Lock()
			if c.lastUID < maxUID {
				c.lastUID = maxUID
			}
			c.mu.Unlock()
		}
		return
	}
}

func (c *EmailChannel) streamFetchEmail(ctx context.Context, cl *imapclient.Client, uids []imap.UID) (uint32, error) {
	fetchSet := imap.UIDSetNum(uids...)
	bodySection := &imap.FetchItemBodySection{}
	fetchOptions := &imap.FetchOptions{
		Envelope:    true,
		UID:         true,
		BodySection: []*imap.FetchItemBodySection{bodySection},
	}
	fetchCmd := cl.Fetch(fetchSet, fetchOptions)
	defer fetchCmd.Close()
	maxUID := uint32(0)
	for {
		mailMessage := fetchCmd.Next()
		if mailMessage == nil {
			break
		}
		uid := c.parseEmail(ctx, mailMessage)
		// update the max UID
		if uid > maxUID {
			maxUID = uid
		}
		// mark the email as seen
		seenSet := imap.UIDSetNum(imap.UID(uid))
		storeCmd := cl.Store(seenSet, &imap.StoreFlags{
			Op:     imap.StoreFlagsAdd,
			Flags:  []imap.Flag{imap.FlagSeen},
			Silent: true,
		}, nil)
		if storeErr := storeCmd.Close(); storeErr != nil {
			logger.DebugCF("email", "Failed to mark email as seen", map[string]any{
				"uid": uid, "error": storeErr.Error(),
			})
		}
	}
	return maxUID, nil
}

func (c *EmailChannel) parseEmail(ctx context.Context, mailMessage *imapclient.FetchMessageData) uint32 {
	if mailMessage == nil {
		return 0
	}
	var (
		envelope         *imap.Envelope
		uid              imap.UID
		bodyContent      string
		originMediaPaths []string
	)
	for {
		item := mailMessage.Next()
		if item == nil {
			break
		}
		switch i := item.(type) {
		case imapclient.FetchItemDataEnvelope:
			envelope = i.Envelope
		case imapclient.FetchItemDataUID:
			uid = i.UID
		case imapclient.FetchItemDataBodySection:
			// Must read the literal immediately: go-imap v2 blocks the parser until
			// the literal is consumed; otherwise Next() deadlocks and body stays empty.
			if i.Literal == nil {
				continue
			}
			bodyContent, originMediaPaths = c.extractEmailBodyAndAttachments(i.Literal)
		default:
			logger.DebugCF("email", "Unknown item type", map[string]any{"item": item})
		}
	}

	// Extract sender
	senderID := ""
	if len(envelope.From) > 0 {
		from := envelope.From[0]
		if from.Mailbox != "" {
			senderID = fmt.Sprintf("%s@%s", from.Mailbox, from.Host)
		}
	}

	if senderID == "" {
		senderID = "unknown"
	}

	mediaPaths := make([]string, 0, len(originMediaPaths))
	content := ""
	scope := "email:" + senderID + ":" + fmt.Sprintf("%d", uid)
	for _, path := range originMediaPaths {
		mediaStore := c.GetMediaStore()
		if mediaStore != nil {
			ref, err := mediaStore.Store(path, media.MediaMeta{
				Filename: filepath.Base(path),
				Source:   "email",
			}, scope)
			if err != nil {
				logger.ErrorCF("email", "Failed to store attachment",
					map[string]any{"error": err.Error(), "path": path})
				continue
			}
			mediaPaths = append(mediaPaths, ref)
		} else {
			// fallback to local path
			mediaPaths = append(mediaPaths, path)
		}
	}

	// SenderInfo for allow-list and routing (canonical format: email:addr)
	senderInfo := bus.SenderInfo{
		Platform:    "email",
		PlatformID:  senderID,
		CanonicalID: identity.BuildCanonicalID("email", senderID),
	}

	// ChatID is sender email (1:1 conversation)
	chatID := senderID
	messageID := fmt.Sprintf("%d", uid)

	// Build metadata
	metadata := map[string]string{
		"subject":    envelope.Subject,
		"message_id": messageID,
		"date":       envelope.Date.Format(time.RFC3339),
		"platform":   "email",
	}

	if len(envelope.To) > 0 {
		to := envelope.To[0]
		metadata["to"] = fmt.Sprintf("%s@%s", to.Mailbox, to.Host)
	}

	logger.DebugCF("email", "Received message", map[string]any{
		"sender_id": senderID,
		"subject":   envelope.Subject,
		"preview":   utils.Truncate(bodyContent, 50),
	})

	// build content
	if envelope.Subject != "" {
		content = fmt.Sprintf("Subject: %s\n\n", envelope.Subject)
	} else {
		content = "Subject: [No subject]"
	}
	if bodyContent != "" {
		content = fmt.Sprintf("%s%s", content, bodyContent)
	} else {
		content = fmt.Sprintf("%s[no body content]", content)
	}
	// Publish to message bus (attachment local paths in mediaPaths)
	peer := bus.Peer{Kind: "direct", ID: senderID}
	c.HandleMessage(ctx, peer, messageID, senderID, chatID, content, mediaPaths, metadata, senderInfo)

	return uint32(uid)
}

// extractEmailBodyAndAttachments parses body and saves attachments to AttachmentDir; returns body text and local paths.
func (c *EmailChannel) extractEmailBodyAndAttachments(bodyReader imap.LiteralReader) (
	content string, mediaPaths []string,
) {
	if bodyReader == nil {
		return "", nil
	}
	bodyTotalRemainingSize := int64(c.config.BodyPartMaxBytes)
	if bodyTotalRemainingSize <= 0 {
		bodyTotalRemainingSize = int64(defaultBodyPartMaxBytes)
	}
	attachmentTotalRemainingSize := int64(c.config.AttachmentMaxBytes)
	if attachmentTotalRemainingSize <= 0 {
		attachmentTotalRemainingSize = int64(defaultAttachmentMaxBytes)
	}
	if bodyReader.Size() == 0 {
		return "", nil
	}
	if bodyReader.Size() > bodyTotalRemainingSize+attachmentTotalRemainingSize {
		logger.ErrorCF("email", "Body and attachment size exceeds limit",
			map[string]any{"size": bodyReader.Size(), "limit": bodyTotalRemainingSize + attachmentTotalRemainingSize})
		return "[email body and attachment size exceeds limit]", nil
	}

	mr, err := mail.CreateReader(bodyReader)
	if err != nil {
		logger.DebugCF("email", "Failed to create mail reader", map[string]any{"error": err.Error()})
		return "", nil
	}
	defer mr.Close()

	var textParts, htmlParts []string
	var attachmentRefs []string
	attachmentIndex := 0
	saveDir := strings.TrimSpace(c.config.AttachmentDir)

	for {
		if attachmentTotalRemainingSize <= 0 && bodyTotalRemainingSize <= 0 {
			// can't read more parts , break the loop
			break
		}
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.DebugCF("email", "Failed to read email part", map[string]any{"error": err.Error()})
			continue
		}

		contentType := getPartContentType(p.Header)
		isAttachment := isAttachmentPart(p.Header)

		if isAttachment {
			if attachmentTotalRemainingSize <= 0 {
				// if attachment limit is exceeded, skip the attachment
				logger.InfoCF("email", "Attachment limit exceeded, skipping attachment",
					map[string]any{"attachment_total_rest_size": attachmentTotalRemainingSize})
				// discard the attachment body
				_, _ = io.Copy(io.Discard, p.Body)
				continue
			}
			filename := getPartFilename(p.Header)
			if filename == "" {
				filename = fmt.Sprintf("attachment_%d", attachmentIndex)
			}
			attachmentIndex++
			// pre check the attachment size
			size, ok := getPartFileSize(p.Header)
			if ok {
				// size is estimated size, may not be accurate
				// if ok check if the attachment size exceeds the limit
				if size > int64(c.config.AttachmentMaxBytes) {
					logger.DebugCF("email", "Attachment size exceeds limit",
						map[string]any{"size": size, "limit": c.config.AttachmentMaxBytes})
					attachmentRefs = append(attachmentRefs,
						fmt.Sprintf("[attachment: %s (save failed, check attachment_max_bytes in config)]", filename))
					continue
				}
			}

			var localPath string
			var attachmentSize int64
			if saveDir != "" {
				attachmentSize, localPath = c.saveAttachmentToLocal(filename, attachmentTotalRemainingSize, p.Body)
				if localPath != "" {
					logger.DebugCF("email", "Saved attachment",
						map[string]any{"filename": filename, "path": localPath, "size": attachmentSize})
					mediaPaths = append(mediaPaths, localPath)
				} else {
					logger.InfoCF("email", "Failed to save attachment",
						map[string]any{"filename": filename, "error": "save failed"})
				}
				attachmentTotalRemainingSize = attachmentTotalRemainingSize - attachmentSize
			} else {
				attachmentRefs = append(attachmentRefs, fmt.Sprintf("[attachment: %s]", filename))
			}
			continue
		}
		if bodyTotalRemainingSize <= 0 {
			// if limit is 0, skip the body part
			logger.InfoCF("email", "Body limit exceeded, skipping body part",
				map[string]any{"body_total_rest_size": bodyTotalRemainingSize})
			// discard the body part
			_, _ = io.Copy(io.Discard, p.Body)
			continue
		}
		limitedBody := io.LimitReader(p.Body, bodyTotalRemainingSize+1)
		body, err := io.ReadAll(limitedBody)
		if err != nil || len(body) == 0 {
			continue
		}
		if len(body) > int(bodyTotalRemainingSize) {
			// if limit is exceeded, append the body part and a warning message
			textParts = append(
				textParts,
				strings.TrimSpace(string(body[:bodyTotalRemainingSize])),
			)
			logger.InfoCF("email", "Body part exceeds size limit, skipping body part",
				map[string]any{"rest_size": bodyTotalRemainingSize})
			bodyTotalRemainingSize = 0
			continue
		}
		bodyTotalRemainingSize = bodyTotalRemainingSize - int64(len(body))
		bodyStr := strings.TrimSpace(string(body))
		if bodyStr == "" {
			continue
		}

		switch {
		case strings.HasPrefix(contentType, "text/plain"):
			textParts = append(textParts, bodyStr)
		case strings.HasPrefix(contentType, "text/html"):
			htmlParts = append(htmlParts, bodyStr)
		case strings.HasPrefix(contentType, "text/"):
			textParts = append(textParts, bodyStr)
		}
	}

	var bodyContent string
	if len(textParts) > 0 {
		bodyContent = strings.TrimSpace(strings.Join(textParts, "\n\n"))
	} else if len(htmlParts) > 0 {
		bodyContent = c.extractTextFromHTML(strings.Join(htmlParts, "\n\n"))
	}

	if bodyContent == "" && len(attachmentRefs) == 0 {
		return "[Empty email]", mediaPaths
	}
	if bodyContent == "" {
		bodyContent = "[attachments only]"
	}
	if len(attachmentRefs) > 0 {
		bodyContent = bodyContent + "\n\n" + strings.Join(attachmentRefs, "\n")
	}
	return bodyContent, mediaPaths
}

// saveAttachmentToLocal writes the attachment stream to AttachmentDir with size limit; returns local path or empty on failure or if over limit.
// return the size of the attachment and the local path
func (c *EmailChannel) saveAttachmentToLocal(filename string,
	limit int64,
	r io.Reader,
) (int64, string) {
	dir := strings.TrimSpace(c.config.AttachmentDir)
	if dir == "" {
		return 0, ""
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		logger.DebugCF("email", "Failed to create attachment dir", map[string]any{"error": err.Error(), "dir": dir})
		return 0, ""
	}
	safeName := utils.SanitizeFilename(filename)
	if safeName == "" {
		safeName = "attachment"
	}
	ext := filepath.Ext(safeName)
	localName := fmt.Sprintf("%s%s", strings.TrimSuffix(safeName, ext), ext)
	localPath := filepath.Join(dir, localName)
	// check if the file exists
	if _, err := os.Stat(localPath); err == nil {
		// file exists,  add uuid to the filename
		localName = fmt.Sprintf("%s_%s%s", uuid.New().String(), strings.TrimSuffix(safeName, ext), ext)
		localPath = filepath.Join(dir, localName)
	}
	f, err := os.Create(localPath)
	if err != nil {
		logger.DebugCF(
			"email",
			"Failed to create attachment file",
			map[string]any{"error": err.Error(), "path": localPath},
		)
		return 0, ""
	}
	defer f.Close()
	// +1 to detect if the attachment exceeds the limit
	limited := io.LimitReader(r, limit+1)
	n, err := io.Copy(f, limited)
	if err != nil {
		_ = os.Remove(localPath)
		logger.DebugCF("email", "Failed to write attachment", map[string]any{"error": err.Error(), "path": localPath})
		return 0, ""
	}
	if n > limit {
		_ = os.Remove(localPath)
		logger.DebugCF(
			"email",
			"Attachment exceeds size limit, skipped",
			map[string]any{"path": localPath, "limit": limit},
		)
		return 0, ""
	}
	return n, localPath
}

// getPartFilename gets the attachment filename from MIME part header and decodes RFC 2047 (e.g. =?GBK?Q?...?=) to UTF-8.
func getPartFilename(h mail.PartHeader) string {
	if h == nil {
		return ""
	}
	var raw string
	if ah, ok := h.(*mail.AttachmentHeader); ok {
		s, _ := ah.Filename()
		raw = strings.TrimSpace(s)
	} else {
		disp := h.Get("Content-Disposition")
		if disp == "" {
			return ""
		}
		raw = parseFilenameFromDisposition(disp)
	}
	if raw == "" {
		return ""
	}
	return decodeRFC2047Filename(raw)
}

func getPartFileSize(h mail.PartHeader) (int64, bool) {
	if h == nil {
		return 0, false
	}
	disp := h.Get("Content-Disposition")
	if disp == "" {
		return 0, false
	}
	raw := parseFilenameFromDisposition(disp)
	if raw == "" {
		return 0, false
	}
	return getPartFileSizeFromDisposition(disp)
}

func getPartFileSizeFromDisposition(disp string) (int64, bool) {
	dispLower := strings.ToLower(disp)
	if !strings.Contains(dispLower, "attachment") && !strings.Contains(dispLower, "inline") {
		return 0, false
	}
	const fn = "size="
	i := strings.Index(dispLower, fn)
	if i < 0 {
		return 0, false
	}
	disp = disp[i+len(fn):]
	disp = strings.TrimSpace(disp)
	if disp == "" {
		return 0, false
	}
	size, err := strconv.ParseInt(disp, 10, 64)
	if err != nil {
		return 0, false
	}
	return size, true
}

// parseFilenameFromDisposition parses the filename= value from Content-Disposition header.
func parseFilenameFromDisposition(disp string) string {
	dispLower := strings.ToLower(disp)
	if !strings.Contains(dispLower, "attachment") && !strings.Contains(dispLower, "inline") {
		return ""
	}
	const fn = "filename="
	i := strings.Index(dispLower, fn)
	if i < 0 {
		return ""
	}

	disp = disp[i+len(fn):]
	disp = strings.TrimLeft(disp, " \t")
	if len(disp) >= 2 && (disp[0] == '"' || disp[0] == '\'') {
		end := strings.IndexByte(disp[1:], disp[0])
		if end >= 0 {
			return strings.TrimSpace(disp[1 : 1+end])
		}
	}
	if idx := strings.IndexAny(disp, " \t;"); idx > 0 {
		disp = disp[:idx]
	}
	return strings.TrimSpace(disp)
}

// rfc2047WordDecoder decodes =?charset?Q?encoded?= to UTF-8; supports GBK/GB2312.
var rfc2047WordDecoder = &mime.WordDecoder{
	CharsetReader: func(charset string, r io.Reader) (io.Reader, error) {
		charset = strings.ToLower(strings.TrimSpace(charset))
		switch charset {
		case "gbk", "gb2312":
			return simplifiedchinese.GBK.NewDecoder().Reader(r), nil
		default:
			return r, nil
		}
	},
}

func decodeRFC2047Filename(s string) string {
	if s == "" || !strings.Contains(s, "=?") {
		return s
	}
	decoded, err := rfc2047WordDecoder.DecodeHeader(s)
	if err != nil {
		return s
	}
	return strings.TrimSpace(decoded)
}

// getPartContentType returns the Content-Type main type (e.g. "text/plain") from PartHeader.
func getPartContentType(h mail.PartHeader) string {
	if h == nil {
		return ""
	}
	raw := h.Get("Content-Type")
	if raw == "" {
		return ""
	}
	// Take the part before the first semicolon and trim
	if i := strings.IndexByte(raw, ';'); i >= 0 {
		raw = raw[:i]
	}
	return strings.TrimSpace(strings.ToLower(raw))
}

// isAttachmentPart reports whether the part should be treated as an attachment (not shown as body).
func isAttachmentPart(h mail.PartHeader) bool {
	if h == nil {
		return false
	}
	if _, ok := h.(*mail.AttachmentHeader); ok {
		return true
	}
	disp := strings.ToLower(strings.TrimSpace(h.Get("Content-Disposition")))
	if strings.HasPrefix(disp, "attachment") {
		return true
	}
	ct := getPartContentType(h)
	// Non-text/* (e.g. image, PDF) is treated as attachment
	if ct != "" && !strings.HasPrefix(ct, "text/") {
		return true
	}
	return false
}

// extractTextFromHTML strips HTML tags and returns plain text (simple impl, no external HTML lib).
func (c *EmailChannel) extractTextFromHTML(html string) string {
	text := html
	// Remove script and style tags and their content
	text = c.removeTagContent(text, "script")
	text = c.removeTagContent(text, "style")

	// Strip all HTML tags
	var result strings.Builder
	inTag := false
	for i, r := range text {
		if r == '<' {
			inTag = true
			continue
		}
		if r == '>' {
			inTag = false
			// Add space after tag if next char is not space
			if i+1 < len(text) && text[i+1] != ' ' && text[i+1] != '\n' {
				result.WriteRune(' ')
			}
			continue
		}
		if !inTag {
			result.WriteRune(r)
		}
	}

	// Normalize whitespace
	cleaned := strings.TrimSpace(result.String())
	cleaned = strings.ReplaceAll(cleaned, "\n\n\n", "\n\n")
	cleaned = strings.ReplaceAll(cleaned, "  ", " ")

	return cleaned
}

// removeTagContent removes the named tag and its content (finds <tagName>...</tagName> and strips it).
func (c *EmailChannel) removeTagContent(html, tagName string) string {
	startTag := "<" + tagName
	endTag := "</" + tagName + ">"

	for {
		startIdx := strings.Index(strings.ToLower(html), strings.ToLower(startTag))
		if startIdx == -1 {
			break
		}

		// Find end of opening tag
		endIdx := strings.Index(html[startIdx:], ">")
		if endIdx == -1 {
			break
		}
		endIdx += startIdx + 1

		// Find matching closing tag
		closeIdx := strings.Index(strings.ToLower(html[endIdx:]), strings.ToLower(endTag))
		if closeIdx == -1 {
			// No closing tag, remove only the opening tag
			html = html[:startIdx] + html[endIdx:]
		} else {
			closeIdx += endIdx + len(endTag)
			html = html[:startIdx] + html[closeIdx:]
		}
	}

	return html
}
