package channels

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"mime"
	"net"
	"net/smtp"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	charset "github.com/emersion/go-message/charset"
	"github.com/emersion/go-message/mail"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/utils"
	"golang.org/x/text/encoding/simplifiedchinese"
)

func init() {
	// Register GBK so go-message can decode mail body (e.g. QQ/163 mailboxes); otherwise "unhandled charset \"gbk\"".
	charset.RegisterEncoding("gbk", simplifiedchinese.GBK)
}

type EmailChannel struct {
	*BaseChannel
	config      config.EmailConfig
	imapClient  *client.Client
	lastUID     uint32
	mu          sync.Mutex
	cancel      context.CancelFunc
	checkTicker *time.Ticker
}

func NewEmailChannel(cfg config.EmailConfig, bus *bus.MessageBus) (*EmailChannel, error) {
	base := NewBaseChannel("email", cfg, bus, cfg.AllowFrom)
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

	if err := c.connect(); err != nil {
		cancel()
		return fmt.Errorf("failed to connect to IMAP server: %w", err)
	}

	c.setRunning(true)
	logger.InfoC("email", "Email channel started")

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
	if c.imapClient != nil {
		c.imapClient.Logout()
		c.imapClient = nil
	}
	c.mu.Unlock()

	c.setRunning(false)
	logger.InfoC("email", "Email channel stopped")
	return nil
}

func (c *EmailChannel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	if !c.IsRunning() {
		return fmt.Errorf("email channel not running")
	}
	if strings.TrimSpace(c.config.SMTPServer) == "" {
		return fmt.Errorf("email channel send: SMTP not configured (set smtp_server)")
	}

	from := c.config.Username
	to := strings.TrimSpace(msg.ChatID)
	if to == "" {
		return fmt.Errorf("email channel send: missing recipient (chat_id)")
	}

	// Plain-text message: From / To / Subject / Body (OutboundMessage has no Metadata, use fixed subject)
	subject := "Reply from PicoClaw"
	header := map[string]string{
		"From":         from,
		"To":           to,
		"Subject":      subject,
		"Content-Type": "text/plain; charset=utf-8",
	}
	var raw strings.Builder
	for k, v := range header {
		raw.WriteString(k + ": " + v + "\r\n")
	}
	raw.WriteString("\r\n")
	raw.WriteString(msg.Content)
	body := raw.String()

	port := c.config.SMTPPort
	if port <= 0 {
		port = 465
	}
	addr := fmt.Sprintf("%s:%d", c.config.SMTPServer, port)
	host := c.config.SMTPServer

	if c.config.SMTPUseTLS {
		// Port 465: implicit TLS
		tlsConfig := &tls.Config{ServerName: host}
		conn, err := tls.Dial("tcp", addr, tlsConfig)
		if err != nil {
			return fmt.Errorf("smtp tls dial: %w", err)
		}
		defer conn.Close()
		client, err := smtp.NewClient(conn, host)
		if err != nil {
			return fmt.Errorf("smtp new client: %w", err)
		}
		defer client.Close()
		auth := smtp.PlainAuth("", c.config.Username, c.config.Password, host)
		if err = client.Auth(auth); err != nil {
			return fmt.Errorf("smtp auth: %w", err)
		}
		if err = client.Mail(from); err != nil {
			return fmt.Errorf("smtp mail: %w", err)
		}
		if err = client.Rcpt(to); err != nil {
			return fmt.Errorf("smtp rcpt: %w", err)
		}
		w, err := client.Data()
		if err != nil {
			return fmt.Errorf("smtp data: %w", err)
		}
		if _, err = w.Write([]byte(body)); err != nil {
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
		_ = err
	}
	auth := smtp.PlainAuth("", c.config.Username, c.config.Password, host)
	if err = client.Auth(auth); err != nil {
		return fmt.Errorf("smtp auth: %w", err)
	}
	if err = client.Mail(from); err != nil {
		return fmt.Errorf("smtp mail: %w", err)
	}
	if err = client.Rcpt(to); err != nil {
		return fmt.Errorf("smtp rcpt: %w", err)
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp data: %w", err)
	}
	if _, err = w.Write([]byte(body)); err != nil {
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

	var cl *client.Client
	var err error

	if c.config.UseTLS {
		cl, err = client.DialTLS(address, nil)
	} else {
		cl, err = client.Dial(address)
	}

	if err != nil {
		return err
	}

	// Login
	if err := cl.Login(c.config.Username, c.config.Password); err != nil {
		cl.Logout()
		return err
	}

	c.mu.Lock()
	c.imapClient = cl
	c.mu.Unlock()

	// Select mailbox
	mailbox := c.config.Mailbox
	if mailbox == "" {
		mailbox = "INBOX"
	}

	status, err := cl.Select(mailbox, false)
	if err != nil {
		if strings.Contains(err.Error(), "Unsafe Login") || strings.Contains(err.Error(), "不安全") {
			return fmt.Errorf("failed to select mailbox %s: %w (hint: 163/QQ/126 require app password, not account password)", mailbox, err)
		}
		return fmt.Errorf("failed to select mailbox %s: %w", mailbox, err)
	}

	// First connect: init lastUID from Select's UidNext (max current UID = UidNext-1) to avoid full UidSearch
	if status != nil && status.UidNext > 0 {
		c.mu.Lock()
		c.lastUID = status.UidNext - 1
		c.mu.Unlock()
	} else {
		// Fallback: some servers do not return UidNext, search all to get max UID
		if err := c.syncLastUID(cl); err != nil {
			cl.Logout()
			return fmt.Errorf("failed to sync mailbox UID: %w", err)
		}
	}

	logger.InfoCF("email", "Connected to IMAP server", map[string]interface{}{
		"server":   c.config.IMAPServer,
		"mailbox":  mailbox,
		"last_uid": c.lastUID,
	})

	return nil
}

// syncLastUID fetches the mailbox max UID and sets lastUID so only mail after connect is processed.
func (c *EmailChannel) syncLastUID(cl *client.Client) error {
	criteria := imap.NewSearchCriteria()
	uids, err := cl.UidSearch(criteria)
	if err != nil {
		// Some servers require a condition; UID 1:* means all
		all := new(imap.SeqSet)
		all.AddRange(1, 4294967295)
		criteria.Uid = all
		uids, err = cl.UidSearch(criteria)
		if err != nil {
			return err
		}
	}
	var maxUID uint32
	for _, uid := range uids {
		if uid > maxUID {
			maxUID = uid
		}
	}
	c.mu.Lock()
	c.lastUID = maxUID
	c.mu.Unlock()
	return nil
}

func (c *EmailChannel) checkLoop(ctx context.Context) {
	interval := time.Duration(c.config.CheckInterval) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// Run one check immediately
	c.checkNewEmails()

	if c.config.UseIdle {
		c.runIdleLoop(ctx, interval)
		return
	}

	// Polling mode
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
			c.checkNewEmails()
		}
	}
}

// runIdleLoop uses IMAP IDLE (RFC 2177). When the server pushes a mailbox update (e.g. * EXISTS for new mail),
// we receive it on Client.Updates, close the IDLE stop channel so Idle() returns, then run checkNewEmails().
// If the server does not support IDLE, go-imap falls back to polling with PollInterval.
func (c *EmailChannel) runIdleLoop(ctx context.Context, pollInterval time.Duration) {
	opts := &client.IdleOptions{
		LogoutTimeout: 25 * time.Minute, // restart IDLE periodically to avoid server disconnect
		PollInterval:  pollInterval,     // used when server does not support IDLE
	}
	// Buffered channel for server unilateral updates (EXISTS, EXPUNGE, etc.); prevents client from blocking.
	updatesCh := make(chan client.Update, 32)

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
		if cl.State() != imap.SelectedState {
			if err := c.connect(); err != nil {
				logger.ErrorCF("email", "Failed to reconnect in IDLE loop", map[string]interface{}{"error": err.Error()})
				return
			}
			continue
		}
		cl.Updates = updatesCh
		stop := make(chan struct{})
		idleDone := make(chan error, 1)
		go func() {
			idleDone <- cl.Idle(stop, opts)
		}()
		go func() {
			<-ctx.Done()
			close(stop)
		}()

		select {
		case <-ctx.Done():
			close(stop)
			<-idleDone
			c.mu.Lock()
			if c.imapClient != nil {
				c.imapClient.Updates = nil
			}
			c.mu.Unlock()
			return
		case <-updatesCh:
			// Server sent e.g. * EXISTS (new mail); exit IDLE and check
			close(stop)
			if err := <-idleDone; err != nil {
				c.mu.Lock()
				if c.imapClient != nil {
					c.imapClient.Updates = nil
				}
				c.mu.Unlock()
				logger.ErrorCF("email", "IDLE ended with error after update", map[string]interface{}{"error": err.Error()})
				return
			}
			c.checkNewEmails()
		case err := <-idleDone:
			// Idle returned (timeout restart or error)
			if err != nil {
				c.mu.Lock()
				if c.imapClient != nil {
					c.imapClient.Updates = nil
				}
				c.mu.Unlock()
				logger.ErrorCF("email", "IDLE ended with error", map[string]interface{}{"error": err.Error()})
				return
			}
			c.checkNewEmails()
		}
	}
}

func (c *EmailChannel) checkNewEmails() {
	c.mu.Lock()
	cl := c.imapClient
	lastUID := c.lastUID
	c.mu.Unlock()

	if cl == nil {
		return
	}

	// Check connection state
	if cl.State() != imap.SelectedState {
		// Reconnect
		if err := c.connect(); err != nil {
			logger.ErrorCF("email", "Failed to reconnect to IMAP server", map[string]interface{}{
				"error": err.Error(),
			})
			return
		}
		c.mu.Lock()
		cl = c.imapClient
		c.mu.Unlock()
	}

	// Only process mail after recorded lastUID (search by UID range, not by unread)
	criteria := imap.NewSearchCriteria()
	if lastUID > 0 {
		// Build SeqSet for UID range (lastUID+1 to max)
		seqset := new(imap.SeqSet)
		seqset.AddRange(lastUID+1, 0)
		criteria.Uid = seqset
		criteria.WithoutFlags = []string{imap.SeenFlag}
	} else {
		// First run: fetch only unread
		criteria.WithoutFlags = []string{imap.SeenFlag}
	}

	uids, err := cl.UidSearch(criteria)
	if err != nil {
		logger.ErrorCF("email", "Failed to search emails", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if len(uids) == 0 {
		return
	}

	fetchSet := new(imap.SeqSet)
	fetchSet.AddNum(uids...)

	messages := make(chan *imap.Message, 10)
	done := make(chan error, 1)

	go func() {
		bodySection := &imap.BodySectionName{}
		done <- cl.UidFetch(fetchSet, []imap.FetchItem{
			imap.FetchEnvelope,
			imap.FetchBodyStructure,
			bodySection.FetchItem(),
		}, messages)
	}()

	maxUID := uint32(0)
	for msg := range messages {
		if msg.Uid > maxUID {
			maxUID = msg.Uid
		}

		// Process the message
		c.processEmail(msg)

		// Mark as seen after fully read
		seenSet := new(imap.SeqSet)
		seenSet.AddNum(msg.Uid)
		if err := cl.UidStore(seenSet, imap.FormatFlagsOp(imap.AddFlags, true), []interface{}{imap.SeenFlag}, nil); err != nil {
			logger.DebugCF("email", "Failed to mark email as seen", map[string]interface{}{
				"uid": msg.Uid, "error": err.Error(),
			})
		}
	}

	if err := <-done; err != nil {
		logger.ErrorCF("email", "Failed to fetch emails", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Update last processed UID
	if maxUID > 0 {
		c.mu.Lock()
		c.lastUID = maxUID
		c.mu.Unlock()
	}
}

func (c *EmailChannel) processEmail(msg *imap.Message) {
	if msg == nil {
		return
	}

	envelope := msg.Envelope
	if envelope == nil {
		return
	}

	// Extract sender
	senderID := ""
	if len(envelope.From) > 0 {
		from := envelope.From[0]
		if from.MailboxName != "" {
			senderID = fmt.Sprintf("%s@%s", from.MailboxName, from.HostName)
		}
	}

	if senderID == "" {
		senderID = "unknown"
	}

	// Check allowlist
	if !c.IsAllowed(senderID) {
		logger.DebugCF("email", "Email from unauthorized sender", map[string]interface{}{
			"sender": senderID,
		})
		return
	}

	// Extract body and attachments (attachments saved to AttachmentDir, paths in mediaPaths)
	content, mediaPaths := c.extractEmailBodyAndAttachments(msg)
	if content == "" {
		content = "[empty email body]"
	}

	// ChatID is sender email
	chatID := senderID

	// Build metadata
	metadata := map[string]string{
		"subject":    envelope.Subject,
		"message_id": fmt.Sprintf("%d", msg.Uid),
		"date":       envelope.Date.Format(time.RFC3339),
	}

	if len(envelope.To) > 0 {
		to := envelope.To[0]
		metadata["to"] = fmt.Sprintf("%s@%s", to.MailboxName, to.HostName)
	}

	logger.InfoCF("email", "Email received", map[string]interface{}{
		"sender_id": senderID,
		"subject":   envelope.Subject,
		"preview":   utils.Truncate(content, 80),
	})

	// Publish to message bus (attachment local paths in mediaPaths)
	c.HandleMessage(senderID, chatID, content, mediaPaths, metadata)
}

// extractEmailBodyAndAttachments parses body and saves attachments to AttachmentDir; returns body text and local paths.
func (c *EmailChannel) extractEmailBodyAndAttachments(msg *imap.Message) (content string, mediaPaths []string) {
	if msg == nil {
		return "", nil
	}

	subject := ""
	if msg.Envelope != nil {
		subject = msg.Envelope.Subject
	}

	bodySection := &imap.BodySectionName{}
	bodyReader := msg.GetBody(bodySection)
	if bodyReader == nil {
		logger.DebugCF("email", "No body in FETCH response", map[string]interface{}{"uid": msg.Uid})
		if subject != "" {
			return fmt.Sprintf("Subject: %s\n\n[No body content]", subject), nil
		}
		return "", nil
	}

	mr, err := mail.CreateReader(bodyReader)
	if err != nil {
		logger.DebugCF("email", "Failed to create mail reader", map[string]interface{}{"error": err.Error()})
		if subject != "" {
			return fmt.Sprintf("Subject: %s\n\n[Failed to parse email body]", subject), nil
		}
		return "", nil
	}
	defer mr.Close()

	var textParts, htmlParts []string
	var attachmentRefs []string
	attachmentIndex := 0
	saveDir := strings.TrimSpace(c.config.AttachmentDir)

	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.DebugCF("email", "Failed to read email part", map[string]interface{}{"error": err.Error()})
			continue
		}

		contentType := getPartContentType(p.Header)
		isAttachment := isAttachmentPart(p.Header)

		if isAttachment {
			filename := getPartFilename(p.Header)
			if filename == "" {
				filename = fmt.Sprintf("attachment_%d", attachmentIndex)
			}
			attachmentIndex++

			var localPath string
			if saveDir != "" {
				localPath = c.saveAttachmentToLocal(msg.Uid, attachmentIndex, filename, p.Body)
				if localPath != "" {
					mediaPaths = append(mediaPaths, localPath)
					attachmentRefs = append(attachmentRefs, fmt.Sprintf("[attachment: %s]", filepath.Base(localPath)))
				} else {
					attachmentRefs = append(attachmentRefs, fmt.Sprintf("[attachment: %s (save failed)]", filename))
				}
			} else {
				attachmentRefs = append(attachmentRefs, fmt.Sprintf("[attachment: %s]", filename))
			}
			continue
		}

		body, err := io.ReadAll(p.Body)
		if err != nil || len(body) == 0 {
			continue
		}
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
		if subject != "" {
			return fmt.Sprintf("Subject: %s\n\n[No body content]", subject), mediaPaths
		}
		return "[Empty email]", mediaPaths
	}
	if bodyContent == "" {
		bodyContent = "[attachments only]"
	}
	if len(attachmentRefs) > 0 {
		bodyContent = bodyContent + "\n\n" + strings.Join(attachmentRefs, "\n")
	}
	if subject != "" {
		bodyContent = fmt.Sprintf("Subject: %s\n\n%s", subject, bodyContent)
	}
	return bodyContent, mediaPaths
}

// saveAttachmentToLocal writes the attachment stream to AttachmentDir; returns local path or empty on failure.
func (c *EmailChannel) saveAttachmentToLocal(uid uint32, index int, filename string, r io.Reader) string {
	dir := strings.TrimSpace(c.config.AttachmentDir)
	if dir == "" {
		return ""
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		logger.DebugCF("email", "Failed to create attachment dir", map[string]interface{}{"error": err.Error(), "dir": dir})
		return ""
	}
	safeName := utils.SanitizeFilename(filename)
	if safeName == "" {
		safeName = "attachment"
	}
	ext := filepath.Ext(safeName)
	if ext == "" && filename != "" {
		ext = filepath.Ext(filename)
	}
	localName := fmt.Sprintf("%d_%d_%s%s", uid, index, strings.TrimSuffix(safeName, ext), ext)
	localPath := filepath.Join(dir, localName)
	f, err := os.Create(localPath)
	if err != nil {
		logger.DebugCF("email", "Failed to create attachment file", map[string]interface{}{"error": err.Error(), "path": localPath})
		return ""
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		_ = os.Remove(localPath)
		logger.DebugCF("email", "Failed to write attachment", map[string]interface{}{"error": err.Error(), "path": localPath})
		return ""
	}
	return localPath
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
