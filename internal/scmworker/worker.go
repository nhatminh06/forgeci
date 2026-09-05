// Package scmworker processes durable SCM deliveries outside webhook requests.
package scmworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/scm"
	gittransport "github.com/nhatminh06/forgeci/internal/scm/git"
	"github.com/nhatminh06/forgeci/internal/store"
)

const (
	DefaultLease       = 2 * time.Minute
	DefaultPoll        = 250 * time.Millisecond
	DefaultBaseBackoff = time.Second
	DefaultMaxBackoff  = time.Minute
	DefaultMaxAttempts = 5
)

type PrepareFunc func(context.Context, scm.Repository, scm.Delivery) (gittransport.Prepared, error)

type Config struct {
	Store       store.SCMWorkerStore
	Prepare     PrepareFunc
	Notify      func()
	WorkerID    string
	Concurrency int
	Lease       time.Duration
	Poll        time.Duration
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	MaxAttempts int
	Now         func() time.Time
}

type Worker struct {
	cfg    Config
	cancel context.CancelFunc
	done   chan struct{}
}

func New(cfg Config) (*Worker, error) {
	if cfg.Store == nil || cfg.Prepare == nil {
		return nil, errors.New("SCM worker store and source preparer are required")
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 2
	}
	if cfg.Lease == 0 {
		cfg.Lease = DefaultLease
	}
	if cfg.Poll == 0 {
		cfg.Poll = DefaultPoll
	}
	if cfg.BaseBackoff == 0 {
		cfg.BaseBackoff = DefaultBaseBackoff
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = DefaultMaxBackoff
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = uuid.NewString()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Concurrency < 1 || cfg.Lease <= 0 || cfg.Poll <= 0 || cfg.BaseBackoff <= 0 || cfg.MaxBackoff < cfg.BaseBackoff || cfg.MaxAttempts < 1 {
		return nil, errors.New("invalid SCM worker configuration")
	}
	return &Worker{cfg: cfg, done: make(chan struct{})}, nil
}

func (w *Worker) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	w.cancel = cancel
	remaining := make(chan struct{}, w.cfg.Concurrency)
	for i := 0; i < w.cfg.Concurrency; i++ {
		go func() {
			defer func() { remaining <- struct{}{} }()
			w.loop(ctx)
		}()
	}
	go func() {
		for i := 0; i < w.cfg.Concurrency; i++ {
			<-remaining
		}
		close(w.done)
	}()
}

func (w *Worker) Close() {
	if w.cancel != nil {
		w.cancel()
		<-w.done
	}
}

func (w *Worker) loop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Poll)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		delivery, err := w.cfg.Store.ClaimSCMDelivery(ctx, w.cfg.WorkerID, w.cfg.Now().UTC(), w.cfg.Lease)
		if err == nil && delivery != nil {
			w.process(ctx, *delivery)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) process(parent context.Context, delivery scm.Delivery) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stopRenew := make(chan struct{})
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		ticker := time.NewTicker(w.cfg.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenew:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.cfg.Store.RenewSCMDeliveryClaim(ctx, delivery.ID, delivery.ClaimToken, w.cfg.Now().UTC(), w.cfg.Lease); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	finishRenewal := func() {
		close(stopRenew)
		<-renewed
	}

	if _, err := w.cfg.Store.GetSCMRunTriggerByDelivery(ctx, delivery.ID); err == nil {
		finishRenewal()
		_ = w.cfg.Store.CompleteSCMDelivery(context.WithoutCancel(parent), delivery.ID, delivery.ClaimToken, scm.DeliveryProcessed)
		if w.cfg.Notify != nil {
			w.cfg.Notify()
		}
		return
	} else if !errors.Is(err, store.ErrNotFound) {
		finishRenewal()
		w.fail(parent, delivery, scm.Transient(err))
		return
	}
	repository, err := w.cfg.Store.GetSCMRepository(ctx, delivery.RepositoryID)
	if err != nil || !repository.Enabled {
		if err == nil {
			err = errors.New("SCM repository is disabled")
		}
		finishRenewal()
		w.fail(parent, delivery, scm.Permanent(err))
		return
	}
	prepared, err := w.cfg.Prepare(ctx, *repository, delivery)
	if err != nil {
		finishRenewal()
		w.fail(parent, delivery, err)
		return
	}
	runIn := createRun(prepared)
	triggerIn := scm.RunTrigger{DeliveryID: delivery.ID, RepositoryID: repository.ID, RunID: runIn.ID, Provider: string(prepared.Provider), CommitSHA: prepared.GitCommitSHA, Ref: prepared.Ref, PullRequestNumber: prepared.PullRequestNumber, InstallationID: delivery.InstallationID}
	_, _, err = w.cfg.Store.CreateSCMRun(ctx, delivery.ClaimToken, runIn, triggerIn)
	finishRenewal()
	if err != nil {
		w.fail(parent, delivery, scm.Transient(err))
		return
	}
	if err := w.cfg.Store.CompleteSCMDelivery(context.WithoutCancel(parent), delivery.ID, delivery.ClaimToken, scm.DeliveryProcessed); err != nil {
		return
	}
	if w.cfg.Notify != nil {
		w.cfg.Notify()
	}
}

func createRun(prepared gittransport.Prepared) store.CreateRun {
	sum := sha256.Sum256(prepared.PipelineYAML)
	meta := prepared.Snapshot
	in := store.CreateRun{ID: uuid.NewString(), PipelineFile: prepared.PipelinePath, PipelineYAML: append([]byte(nil), prepared.PipelineYAML...), PipelineSHA256: hex.EncodeToString(sum[:]), Workspace: prepared.Repository, MaxParallel: 1,
		Snapshot: &store.SourceSnapshot{SourceDigest: meta.SourceDigest, BlobDigest: meta.BlobDigest, Format: meta.Format, ArchiveSizeBytes: meta.ArchiveSizeBytes, LogicalSizeBytes: meta.LogicalSizeBytes, EntryCount: meta.EntryCount, CreatedAt: meta.CreatedAt}}
	for _, name := range prepared.Pipeline.Order {
		job := prepared.Pipeline.Nodes[name].Job
		in.Jobs = append(in.Jobs, store.Job{Name: name, Status: store.JobPending, Image: job.Image})
		for _, dependency := range job.Needs {
			in.Dependencies = append(in.Dependencies, store.JobDependency{JobName: name, DependsOn: dependency})
		}
	}
	return in
}

func (w *Worker) fail(ctx context.Context, delivery scm.Delivery, err error) {
	if err == nil {
		return
	}
	message := err.Error()
	if len(message) > 4096 {
		message = message[:4096]
	}
	var retryAt *time.Time
	if scm.Failure(err) == scm.FailureTransient && delivery.AttemptCount < w.cfg.MaxAttempts {
		delay := w.cfg.BaseBackoff
		for i := 1; i < delivery.AttemptCount && delay < w.cfg.MaxBackoff; i++ {
			delay *= 2
			if delay > w.cfg.MaxBackoff {
				delay = w.cfg.MaxBackoff
			}
		}
		value := w.cfg.Now().UTC().Add(delay)
		retryAt = &value
	}
	_ = w.cfg.Store.FailSCMDelivery(context.WithoutCancel(ctx), delivery.ID, delivery.ClaimToken, retryAt, fmt.Sprintf("%s", message))
}
