package executor

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/nhatminh06/forgeci/internal/config"
)

const (
	workspace       = "/workspace"
	cleanupTimeout  = 15 * time.Second
	managedLabelKey = "forgeci.managed"

	defaultMemoryBytes = int64(1 << 30)
	defaultNanoCPUs    = int64(2_000_000_000)
	defaultPidsLimit   = int64(256)
)

type DockerLimits struct {
	MemoryBytes int64
	NanoCPUs    int64
	PidsLimit   int64
}

type dockerAPI interface {
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
	ImagePull(context.Context, string, client.ImagePullOptions) (client.ImagePullResponse, error)
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ExecCreate(context.Context, string, client.ExecCreateOptions) (client.ExecCreateResult, error)
	ExecAttach(context.Context, string, client.ExecAttachOptions) (client.ExecAttachResult, error)
	ExecInspect(context.Context, string, client.ExecInspectOptions) (client.ExecInspectResult, error)
	ContainerKill(context.Context, string, client.ContainerKillOptions) (client.ContainerKillResult, error)
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
}

type Docker struct {
	client    dockerAPI
	directory string
	limits    DockerLimits
}

func NewDocker(directory string) (*Docker, error) {
	return NewDockerWithLimits(directory, DockerLimits{})
}

func NewDockerWithLimits(directory string, limits DockerLimits) (*Docker, error) {
	limits, err := normalizeDockerLimits(limits)
	if err != nil {
		return nil, err
	}
	api, err := client.New(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("configure Docker client: %w", err)
	}
	return &Docker{client: api, directory: directory, limits: limits}, nil
}

func normalizeDockerLimits(limits DockerLimits) (DockerLimits, error) {
	if limits.MemoryBytes < 0 || limits.NanoCPUs < 0 || limits.PidsLimit < 0 {
		return DockerLimits{}, fmt.Errorf("Docker resource limits must not be negative")
	}
	if limits.MemoryBytes == 0 {
		limits.MemoryBytes = defaultMemoryBytes
	}
	if limits.NanoCPUs == 0 {
		limits.NanoCPUs = defaultNanoCPUs
	}
	if limits.PidsLimit == 0 {
		limits.PidsLimit = defaultPidsLimit
	}
	return limits, nil
}

func (d *Docker) ReconcileOrphans(ctx context.Context) error {
	cleanupCtx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	listed, err := d.client.ContainerList(cleanupCtx, client.ContainerListOptions{
		All:     true,
		Filters: make(client.Filters).Add("label", managedLabelKey+"=true"),
	})
	if err != nil {
		return fmt.Errorf("list ForgeCI-managed containers: %w", err)
	}
	removed := make([]string, 0, len(listed.Items))
	for _, item := range listed.Items {
		if item.Labels[managedLabelKey] != "true" {
			continue
		}
		if _, err := d.client.ContainerRemove(cleanupCtx, item.ID, client.ContainerRemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
			return fmt.Errorf("remove orphaned container %s: %w", item.ID, err)
		}
		removed = append(removed, item.ID)
	}
	log.Printf("reconciled %d orphaned ForgeCI containers: %v", len(removed), removed)
	return nil
}

func (d *Docker) RunJob(ctx context.Context, name string, job config.Job, stdout, stderr io.Writer) (result Result) {
	limits, err := normalizeDockerLimits(d.limits)
	if err != nil {
		return infrastructureError("configure resource limits", err)
	}
	image := *job.Image
	if _, err := d.client.ImageInspect(ctx, image); err != nil {
		if !cerrdefs.IsNotFound(err) {
			return infrastructureError("inspect image", err)
		}
		fmt.Fprintf(stdout, "Pulling Docker image %s...\n", image)
		pull, err := d.client.ImagePull(ctx, image, client.ImagePullOptions{})
		if err != nil {
			return infrastructureError("pull image", err)
		}
		err = pull.Wait(ctx)
		_ = pull.Close()
		if err != nil {
			return infrastructureError("pull image", err)
		}
		fmt.Fprintf(stdout, "Pulled Docker image %s\n", image)
	}

	created, err := d.client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image:      image,
			User:       fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
			Cmd:        []string{"/bin/sh", "-c", "while :; do sleep 3600; done"},
			WorkingDir: workspace,
			Labels: map[string]string{
				managedLabelKey: "true",
				"forgeci.job":   name,
			},
		},
		HostConfig: &container.HostConfig{
			Binds: []string{d.directory + ":" + workspace + ":rw"},
			Resources: container.Resources{
				Memory:    limits.MemoryBytes,
				NanoCPUs:  limits.NanoCPUs,
				PidsLimit: &limits.PidsLimit,
			},
		},
	})
	if err != nil {
		return infrastructureError("create container", err)
	}
	containerID := created.ID
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := d.client.ContainerRemove(cleanupCtx, containerID, client.ContainerRemoveOptions{Force: true}); err != nil {
			if result.Err != nil {
				err = fmt.Errorf("%w (after %v)", err, result.Err)
			}
			result = infrastructureError("remove container", err)
		}
	}()

	if _, err := d.client.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		return infrastructureError("start container", err)
	}
	for _, step := range job.Steps {
		fmt.Fprintf(stdout, "$ %s\n", step.Run)
		stepResult := d.runStep(ctx, containerID, step.Run, stdout, stderr)
		if stepResult.Err != nil {
			return stepResult
		}
	}
	return Result{ExitCode: 0}
}

func (d *Docker) runStep(ctx context.Context, containerID, command string, stdout, stderr io.Writer) Result {
	execResult, err := d.client.ExecCreate(ctx, containerID, client.ExecCreateOptions{
		AttachStdout: true, AttachStderr: true, WorkingDir: workspace,
		Cmd: []string{"/bin/sh", "-c", command},
	})
	if err != nil {
		return infrastructureError("create container exec", err)
	}
	attached, err := d.client.ExecAttach(ctx, execResult.ID, client.ExecAttachOptions{})
	if err != nil {
		return infrastructureError("attach container exec", err)
	}

	copyDone := make(chan error, 1)
	go func() {
		_, err := stdcopy.StdCopy(stdout, stderr, attached.Reader)
		copyDone <- err
	}()
	select {
	case err := <-copyDone:
		attached.Close()
		if ctx.Err() != nil {
			d.kill(containerID)
			return Result{ExitCode: -1, Err: ctx.Err()}
		}
		if err != nil {
			return infrastructureError("stream container exec output", err)
		}
	case <-ctx.Done():
		d.kill(containerID)
		attached.Close()
		<-copyDone
		return Result{ExitCode: -1, Err: ctx.Err()}
	}

	inspected, err := d.client.ExecInspect(ctx, execResult.ID, client.ExecInspectOptions{})
	if err != nil {
		return infrastructureError("inspect container exec", err)
	}
	if inspected.Running {
		return infrastructureError("inspect container exec", fmt.Errorf("exec remained running after output stream closed"))
	}
	if inspected.ExitCode != 0 {
		return Result{ExitCode: inspected.ExitCode, Err: fmt.Errorf("command exited with status %d", inspected.ExitCode)}
	}
	return Result{ExitCode: 0}
}

func (d *Docker) kill(containerID string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_, _ = d.client.ContainerKill(cleanupCtx, containerID, client.ContainerKillOptions{})
}

func infrastructureError(operation string, err error) Result {
	return Result{ExitCode: -1, Err: fmt.Errorf("Docker %s: %w", operation, err)}
}
