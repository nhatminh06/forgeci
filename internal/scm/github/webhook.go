// Package github implements GitHub webhook authentication and normalization.
package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nhatminh06/forgeci/internal/scm"
)

const (
	SignatureHeaderMax = 80
	EventHeaderMax     = 64
	DeliveryHeaderMax  = 256
)

var ErrInvalidSignature = errors.New("invalid GitHub webhook signature")

type Normalized struct {
	Event   scm.Event
	Ignored bool
}

func VerifySignature(secret []byte, value string, body []byte) error {
	if len(secret) == 0 || len(value) != len("sha256=")+64 || !strings.HasPrefix(value, "sha256=") {
		return ErrInvalidSignature
	}
	supplied, err := hex.DecodeString(strings.TrimPrefix(value, "sha256="))
	if err != nil || len(supplied) != sha256.Size {
		return ErrInvalidSignature
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	if !hmac.Equal(supplied, mac.Sum(nil)) {
		return ErrInvalidSignature
	}
	return nil
}

func ValidateHeader(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func Normalize(eventName, deliveryID string, body []byte, receivedAt time.Time) (Normalized, error) {
	if eventName == "ping" {
		return Normalized{Ignored: true}, nil
	}
	if eventName != "push" && eventName != "pull_request" {
		return Normalized{Ignored: true}, nil
	}
	var envelope webhookEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return Normalized{}, fmt.Errorf("invalid GitHub webhook JSON: %w", err)
	}
	fullName, err := scm.NormalizeRepository(scm.GitHub, envelope.Repository.FullName)
	if err != nil {
		return Normalized{}, err
	}
	installationID := ""
	if envelope.Installation.ID > 0 {
		installationID = strconv.FormatInt(envelope.Installation.ID, 10)
	}
	event := scm.Event{Provider: string(scm.GitHub), DeliveryID: deliveryID, RepositoryFullName: fullName, InstallationID: installationID, ReceivedAt: receivedAt.UTC()}
	switch eventName {
	case "push":
		if !strings.HasPrefix(envelope.Ref, "refs/heads/") || isZeroSHA(envelope.After) {
			return Normalized{Ignored: true}, nil
		}
		if !validSHA(envelope.After) {
			return Normalized{}, fmt.Errorf("invalid push commit SHA")
		}
		event.EventType, event.CommitSHA, event.Ref = scm.EventPush, strings.ToLower(envelope.After), envelope.Ref
		return Normalized{Event: event}, nil
	case "pull_request":
		action := envelope.Action
		if action != "opened" && action != "reopened" && action != "synchronize" && action != "ready_for_review" && action != "closed" {
			return Normalized{Ignored: true}, nil
		}
		if envelope.PullRequest.Number < 1 || !validSHA(envelope.PullRequest.Head.SHA) || !validRef(envelope.PullRequest.Head.Ref) || !validRef(envelope.PullRequest.Base.Ref) {
			return Normalized{}, fmt.Errorf("invalid pull request webhook")
		}
		event.EventType, event.Action, event.CommitSHA = scm.EventPullRequest, action, strings.ToLower(envelope.PullRequest.Head.SHA)
		event.PullRequestNumber = &envelope.PullRequest.Number
		event.PullRequestHeadRef, event.PullRequestBaseRef = envelope.PullRequest.Head.Ref, envelope.PullRequest.Base.Ref
		return Normalized{Event: event, Ignored: envelope.PullRequest.Draft && action != "ready_for_review" || action == "closed"}, nil
	}
	return Normalized{Ignored: true}, nil
}

type webhookEnvelope struct {
	Action       string `json:"action"`
	After        string `json:"after"`
	Ref          string `json:"ref"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	PullRequest struct {
		Number int  `json:"number"`
		Draft  bool `json:"draft"`
		Head   struct {
			SHA string `json:"sha"`
			Ref string `json:"ref"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
	} `json:"pull_request"`
}

func validSHA(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func isZeroSHA(value string) bool {
	return value != "" && strings.Trim(value, "0") == ""
}

func validRef(value string) bool {
	return value != "" && len(value) <= 1024 && !strings.ContainsAny(value, "\x00\r\n")
}
