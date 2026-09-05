package scmworker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/scm"
	"github.com/nhatminh06/forgeci/internal/store"
)

type ReportCheckFunc func(context.Context, scm.Repository, scm.RunTrigger) (string, error)

type CheckConfig struct {
	Store    store.SCMCheckStore
	Report   ReportCheckFunc
	WorkerID string
	Lease    time.Duration
	Poll     time.Duration
	Now      func() time.Time
}

type CheckWorker struct {
	cfg    CheckConfig
	cancel context.CancelFunc
	done   chan struct{}
}

func NewCheckWorker(cfg CheckConfig) (*CheckWorker, error) {
	if cfg.Store == nil || cfg.Report == nil {
		return nil, errors.New("SCM Check store and reporter are required")
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = uuid.NewString()
	}
	if cfg.Lease == 0 {
		cfg.Lease = time.Minute
	}
	if cfg.Poll == 0 {
		cfg.Poll = time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Lease <= 0 || cfg.Poll <= 0 {
		return nil, errors.New("invalid SCM Check worker configuration")
	}
	return &CheckWorker{cfg: cfg, done: make(chan struct{})}, nil
}

func (w *CheckWorker) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	w.cancel = cancel
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(w.cfg.Poll)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			report, err := w.cfg.Store.ClaimSCMCheck(ctx, w.cfg.WorkerID, w.cfg.Now().UTC(), w.cfg.Lease)
			if err == nil && report != nil {
				w.reconcile(ctx, *report)
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (w *CheckWorker) Close() {
	if w.cancel != nil {
		w.cancel()
		<-w.done
	}
}

func (w *CheckWorker) reconcile(ctx context.Context, report scm.RunTrigger) {
	repository, err := w.cfg.Store.GetSCMRepository(ctx, report.RepositoryID)
	if err == nil {
		var id string
		id, err = w.cfg.Report(ctx, *repository, report)
		if err == nil {
			_ = w.cfg.Store.CompleteSCMCheck(context.WithoutCancel(ctx), report.ID, report.CheckClaimToken, id, report.DesiredCheckStatus, report.DesiredCheckConclusion)
			return
		}
	}
	delay := time.Second
	for i := 1; i < report.CheckAttemptCount && delay < time.Minute; i++ {
		delay *= 2
		if delay > time.Minute {
			delay = time.Minute
		}
	}
	message := err.Error()
	if len(message) > 4096 {
		message = message[:4096]
	}
	_ = w.cfg.Store.FailSCMCheck(context.WithoutCancel(ctx), report.ID, report.CheckClaimToken, w.cfg.Now().UTC().Add(delay), message)
}
