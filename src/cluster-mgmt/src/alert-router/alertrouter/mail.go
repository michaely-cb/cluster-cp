package alertrouter

import (
	"fmt"

	mail "github.com/wneessen/go-mail"
)

// MailSender defines the interface for sending mail
type MailSender interface {
	SendMail(to []string, subject string, body string) error
}

// mailClient wraps the go-mail client
type mailClient struct {
	client *mail.Client
	sender string
}

func NewMailSender(client *mail.Client, sender string) *mailClient {
	return &mailClient{client: client, sender: sender}
}

func (c *mailClient) SendMail(to []string, subject string, body string) (err error) {
	msg := mail.NewMsg()
	msg.To(to...)
	msg.From(c.sender)
	msg.Subject(subject)
	msg.SetBodyString(mail.TypeTextPlain, body)
	if err := c.client.DialAndSend(msg); err != nil {
		return fmt.Errorf("failed to send mail: %w", err)
	}
	return err // err will be nil here, or the panic error
}
