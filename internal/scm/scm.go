// Package scm defines provider-neutral source-control domain values.
package scm

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

type FailureKind uint8

const (
	FailureTransient FailureKind = iota + 1
	FailurePermanent
)

type ProcessingError struct {
	Kind FailureKind
	Err  error
}

func (e *ProcessingError) Error() string { return e.Err.Error() }
func (e *ProcessingError) Unwrap() error { return e.Err }
func Transient(err error) error          { return &ProcessingError{Kind: FailureTransient, Err: err} }
func Permanent(err error) error          { return &ProcessingError{Kind: FailurePermanent, Err: err} }
func Failure(err error) FailureKind {
	var target *ProcessingError
	if errors.As(err, &target) {
		return target.Kind
	}
	return FailureTransient
}

type Provider string

const GitHub Provider = "github"

type Repository struct {
	ID           string    `json:"id"`
	Provider     Provider  `json:"provider"`
	FullName     string    `json:"full_name"`
	PipelinePath string    `json:"pipeline"`
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type EventType string

const (
	EventPush        EventType = "push"
	EventPullRequest EventType = "pull_request"
)

type DeliveryStatus string

const (
	DeliveryPending    DeliveryStatus = "PENDING"
	DeliveryProcessing DeliveryStatus = "PROCESSING"
	DeliveryProcessed  DeliveryStatus = "PROCESSED"
	DeliveryFailed     DeliveryStatus = "FAILED"
	DeliveryIgnored    DeliveryStatus = "IGNORED"
)

type Event struct {
	Provider, DeliveryID, RepositoryID, RepositoryFullName string
	EventType                                              EventType
	Action, InstallationID, CommitSHA, Ref                 string
	PullRequestNumber                                      *int
	PullRequestHeadRef, PullRequestBaseRef, PayloadSHA256  string
	ReceivedAt                                             time.Time
}

type Delivery struct {
	ID, Provider, DeliveryID, RepositoryID, EventType, Action, InstallationID string
	CommitSHA, Ref, PayloadSHA256                                             string
	PullRequestNumber                                                         *int
	PullRequestHeadRef, PullRequestBaseRef                                    string
	Status                                                                    DeliveryStatus
	AttemptCount                                                              int
	NextAttemptAt                                                             *time.Time
	LastError                                                                 *string
	ClaimToken, ClaimedBy                                                     string
	ClaimExpiresAt                                                            *time.Time
	ReceivedAt, ProcessedAt                                                   time.Time
}

type RunTrigger struct {
	ID, DeliveryID, RepositoryID, RunID, Provider, CommitSHA, Ref, InstallationID string
	PullRequestNumber                                                             *int
	CheckRunID, CheckState, LastCheckError                                        *string
	ExternalID, DesiredCheckStatus                                                string
	DesiredCheckConclusion, LastCheckConclusion                                   *string
	CheckClaimToken, CheckClaimedBy                                               string
	CheckClaimExpiresAt, NextCheckAttemptAt                                       *time.Time
	CheckAttemptCount                                                             int
	CreatedAt, UpdatedAt                                                          time.Time
}

func NormalizeRepository(provider Provider, fullName string) (string, error) {
	if provider != GitHub {
		return "", fmt.Errorf("unsupported SCM provider %q", provider)
	}
	if len(fullName) == 0 || len(fullName) > 256 || strings.TrimSpace(fullName) != fullName || strings.ContainsAny(fullName, " \t\r\n\x00?#:\\@") {
		return "", fmt.Errorf("invalid repository name")
	}
	parts := strings.Split(fullName, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || parts[0] == "." || parts[0] == ".." || parts[1] == "." || parts[1] == ".." || strings.Contains(parts[0], "..") || strings.Contains(parts[1], "..") {
		return "", fmt.Errorf("invalid repository name")
	}
	return strings.ToLower(fullName), nil
}

func ValidatePipelinePath(value string) (string, error) {
	if value == "" || len(value) > 512 || strings.ContainsRune(value, '\x00') || strings.Contains(value, "\\") || strings.HasPrefix(value, "/") || (len(value) >= 2 && value[1] == ':') {
		return "", fmt.Errorf("invalid pipeline path")
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || clean != value || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("invalid pipeline path")
	}
	return clean, nil
}
