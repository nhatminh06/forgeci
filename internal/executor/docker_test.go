package executor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"os"
	"strings"
	"sync"
	"testing"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"
	"github.com/nhatminh06/forgeci/internal/config"
)

type fakePull struct{ err error }

func (*fakePull) Read([]byte) (int, error)     { return 0, io.EOF }
func (*fakePull) Close() error                 { return nil }
func (p *fakePull) Wait(context.Context) error { return p.err }
func (*fakePull) JSONMessages(context.Context) iter.Seq2[jsonstream.Message, error] {
	return func(func(jsonstream.Message, error) bool) {}
}

type fakeDocker struct {
	mu          sync.Mutex
	events      []string
	missing     bool
	inspectErr  error
	pullErr     error
	createErr   error
	startErr    error
	createOpts  []client.ContainerCreateOptions
	containers  []container.Summary
	listOpts    []client.ContainerListOptions
	removedIDs  []string
	removeOpts  []client.ContainerRemoveOptions
	exitCodes   []int
	outputs     [][2]string
	kills       int
	removes     int
	blockOutput bool
}

func (f *fakeDocker) event(event string) {
	f.mu.Lock()
	f.events = append(f.events, event)
	f.mu.Unlock()
}
func (f *fakeDocker) ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error) {
	f.event("inspect-image")
	if f.inspectErr != nil {
		return client.ImageInspectResult{}, f.inspectErr
	}
	if f.missing {
		return client.ImageInspectResult{}, cerrdefs.ErrNotFound.WithMessage("missing")
	}
	return client.ImageInspectResult{}, nil
}
func (f *fakeDocker) ImagePull(context.Context, string, client.ImagePullOptions) (client.ImagePullResponse, error) {
	f.event("pull")
	if f.pullErr != nil {
		return nil, f.pullErr
	}
	return &fakePull{}, nil
}
func (f *fakeDocker) ContainerList(_ context.Context, options client.ContainerListOptions) (client.ContainerListResult, error) {
	f.event("list")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listOpts = append(f.listOpts, options)
	return client.ContainerListResult{Items: f.containers}, nil
}
func (f *fakeDocker) ContainerCreate(_ context.Context, options client.ContainerCreateOptions) (client.ContainerCreateResult, error) {
	f.event("create")
	f.mu.Lock()
	f.createOpts = append(f.createOpts, options)
	f.mu.Unlock()
	if f.createErr != nil {
		return client.ContainerCreateResult{}, f.createErr
	}
	return client.ContainerCreateResult{ID: "container"}, nil
}
func (f *fakeDocker) ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error) {
	f.event("start")
	return client.ContainerStartResult{}, f.startErr
}
func (f *fakeDocker) ExecCreate(context.Context, string, client.ExecCreateOptions) (client.ExecCreateResult, error) {
	f.mu.Lock()
	id := len(f.events)
	f.mu.Unlock()
	f.event("exec-create")
	return client.ExecCreateResult{ID: string(rune('a' + id))}, nil
}
func (f *fakeDocker) ExecAttach(ctx context.Context, _ string, _ client.ExecAttachOptions) (client.ExecAttachResult, error) {
	f.event("exec-attach")
	reader, writer := net.Pipe()
	go func() {
		defer writer.Close()
		if f.blockOutput {
			<-ctx.Done()
			return
		}
		f.mu.Lock()
		var output [2]string
		if len(f.outputs) > 0 {
			output, f.outputs = f.outputs[0], f.outputs[1:]
		}
		f.mu.Unlock()
		writeFrame(writer, stdcopy.Stdout, output[0])
		writeFrame(writer, stdcopy.Stderr, output[1])
	}()
	return client.ExecAttachResult{HijackedResponse: client.HijackedResponse{Conn: reader, Reader: bufio.NewReader(reader)}}, nil
}

func writeFrame(w io.Writer, stream stdcopy.StdType, text string) {
	header := make([]byte, 8)
	header[0] = byte(stream)
	binary.BigEndian.PutUint32(header[4:], uint32(len(text)))
	_, _ = w.Write(header)
	_, _ = io.WriteString(w, text)
}
func (f *fakeDocker) ExecInspect(context.Context, string, client.ExecInspectOptions) (client.ExecInspectResult, error) {
	f.event("exec-inspect")
	f.mu.Lock()
	defer f.mu.Unlock()
	code := 0
	if len(f.exitCodes) > 0 {
		code, f.exitCodes = f.exitCodes[0], f.exitCodes[1:]
	}
	return client.ExecInspectResult{ExitCode: code}, nil
}
func (f *fakeDocker) ContainerKill(context.Context, string, client.ContainerKillOptions) (client.ContainerKillResult, error) {
	f.mu.Lock()
	f.kills++
	f.mu.Unlock()
	f.event("kill")
	return client.ContainerKillResult{}, nil
}
func (f *fakeDocker) ContainerRemove(_ context.Context, id string, options client.ContainerRemoveOptions) (client.ContainerRemoveResult, error) {
	f.mu.Lock()
	f.removes++
	f.removedIDs = append(f.removedIDs, id)
	f.removeOpts = append(f.removeOpts, options)
	f.mu.Unlock()
	f.event("remove")
	return client.ContainerRemoveResult{}, nil
}

func TestDockerReconcileOrphansRemovesOnlyManagedContainers(t *testing.T) {
	fake := &fakeDocker{containers: []container.Summary{
		{ID: "managed-running", Labels: map[string]string{managedLabelKey: "true"}},
		{ID: "unmanaged", Labels: map[string]string{"other": "true"}},
		{ID: "managed-stopped", Labels: map[string]string{managedLabelKey: "true"}},
	}}
	docker := &Docker{client: fake}
	if err := docker.ReconcileOrphans(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fake.listOpts) != 1 || !fake.listOpts[0].All || !fake.listOpts[0].Filters["label"][managedLabelKey+"=true"] {
		t.Fatalf("list options=%+v", fake.listOpts)
	}
	if got := strings.Join(fake.removedIDs, ","); got != "managed-running,managed-stopped" {
		t.Fatalf("removed=%q", got)
	}
	for _, options := range fake.removeOpts {
		if !options.Force {
			t.Fatalf("orphan removal was not forced: %+v", options)
		}
	}
}

func TestDockerContainerCreateAppliesConfiguredResourceLimits(t *testing.T) {
	fake := &fakeDocker{}
	limits := DockerLimits{MemoryBytes: 768 << 20, NanoCPUs: 1_500_000_000, PidsLimit: 96}
	docker := &Docker{client: fake, directory: t.TempDir(), limits: limits}
	result := docker.RunJob(context.Background(), "limited", imageJob("image", "true"), io.Discard, io.Discard)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	resources := fake.createOpts[0].HostConfig.Resources
	if resources.Memory != limits.MemoryBytes || resources.NanoCPUs != limits.NanoCPUs || resources.PidsLimit == nil || *resources.PidsLimit != limits.PidsLimit {
		t.Fatalf("resources=%+v", resources)
	}
}

func TestDockerContainerCreateAppliesDefaultResourceLimits(t *testing.T) {
	fake := &fakeDocker{}
	docker := &Docker{client: fake, directory: t.TempDir()}
	result := docker.RunJob(context.Background(), "default-limits", imageJob("image", "true"), io.Discard, io.Discard)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	resources := fake.createOpts[0].HostConfig.Resources
	if resources.Memory != defaultMemoryBytes || resources.NanoCPUs != defaultNanoCPUs || resources.PidsLimit == nil || *resources.PidsLimit != defaultPidsLimit {
		t.Fatalf("resources=%+v", resources)
	}
}

func imageJob(image string, commands ...string) config.Job {
	steps := make([]config.Step, len(commands))
	for i, command := range commands {
		steps[i] = config.Step{Run: command}
	}
	return config.Job{Image: &image, Steps: steps}
}

func TestDockerUsesOneContainerForAllStepsAndCleansUp(t *testing.T) {
	fake := &fakeDocker{outputs: [][2]string{{"one-out", "one-err"}, {"two-out", "two-err"}}}
	docker := &Docker{client: fake, directory: t.TempDir()}
	var stdout, stderr bytes.Buffer
	result := docker.RunJob(context.Background(), "test", imageJob("alpine:3.22", "one", "two"), &stdout, &stderr)
	if result.Err != nil {
		t.Fatalf("RunJob() = %+v", result)
	}
	got := strings.Join(fake.events, ",")
	want := "inspect-image,create,start,exec-create,exec-attach,exec-inspect,exec-create,exec-attach,exec-inspect,remove"
	if got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
	if fake.removes != 1 || !strings.Contains(stdout.String(), "one-out") || !strings.Contains(stderr.String(), "two-err") {
		t.Fatalf("removes=%d stdout=%q stderr=%q", fake.removes, stdout.String(), stderr.String())
	}
	opts := fake.createOpts[0]
	if opts.Config.User != fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()) || opts.Config.WorkingDir != workspace || opts.Config.Labels[managedLabelKey] != "true" || opts.Config.Labels["forgeci.job"] != "test" || len(opts.HostConfig.Binds) != 1 || !strings.HasSuffix(opts.HostConfig.Binds[0], ":/workspace:rw") || opts.HostConfig.Privileged {
		t.Fatalf("unsafe or incomplete container options: %+v", opts)
	}
}

func TestDockerPartialLifecycleFailureStillRemoves(t *testing.T) {
	fake := &fakeDocker{startErr: errors.New("start failed")}
	result := (&Docker{client: fake, directory: t.TempDir()}).RunJob(context.Background(), "test", imageJob("image", "true"), io.Discard, io.Discard)
	if result.ExitCode != -1 || result.Err == nil || fake.removes != 1 {
		t.Fatalf("result=%+v removes=%d events=%v", result, fake.removes, fake.events)
	}
}

func TestDockerPullPathsAndInfrastructureErrors(t *testing.T) {
	t.Run("missing pulls once", func(t *testing.T) {
		fake := &fakeDocker{missing: true}
		result := (&Docker{client: fake, directory: t.TempDir()}).RunJob(context.Background(), "test", imageJob("alpine:3.22", "true"), io.Discard, io.Discard)
		if result.Err != nil || strings.Count(strings.Join(fake.events, ","), "pull") != 1 {
			t.Fatalf("result=%+v events=%v", result, fake.events)
		}
	})
	t.Run("pull failure never creates", func(t *testing.T) {
		fake := &fakeDocker{missing: true, pullErr: errors.New("registry down")}
		result := (&Docker{client: fake, directory: t.TempDir()}).RunJob(context.Background(), "test", imageJob("missing", "true"), io.Discard, io.Discard)
		if result.ExitCode != -1 || result.Err == nil || strings.Contains(strings.Join(fake.events, ","), "create") {
			t.Fatalf("result=%+v events=%v", result, fake.events)
		}
	})
	t.Run("inspect failure never falls back", func(t *testing.T) {
		fake := &fakeDocker{inspectErr: errors.New("daemon down")}
		result := (&Docker{client: fake, directory: t.TempDir()}).RunJob(context.Background(), "test", imageJob("image", "true"), io.Discard, io.Discard)
		if result.ExitCode != -1 || result.Err == nil || len(fake.events) != 1 {
			t.Fatalf("result=%+v events=%v", result, fake.events)
		}
	})
}

func TestDockerExitCodeSkipsRemainingStepsAndRemoves(t *testing.T) {
	fake := &fakeDocker{exitCodes: []int{23}}
	result := (&Docker{client: fake, directory: t.TempDir()}).RunJob(context.Background(), "test", imageJob("image", "exit 23", "must-not-run"), io.Discard, io.Discard)
	if result.ExitCode != 23 || result.Err == nil || strings.Count(strings.Join(fake.events, ","), "exec-create") != 1 || fake.removes != 1 {
		t.Fatalf("result=%+v events=%v removes=%d", result, fake.events, fake.removes)
	}
}

func TestJobRoutesLocalWithoutDocker(t *testing.T) {
	dir := t.TempDir()
	fake := &fakeDocker{}
	router := Job{Local: Local{Directory: dir}, Docker: &Docker{client: fake, directory: dir}}
	result := router.RunJob(context.Background(), "local", config.Job{Steps: []config.Step{{Run: "echo local"}}}, io.Discard, io.Discard)
	if result.Err != nil || len(fake.events) != 0 {
		t.Fatalf("result=%+v Docker events=%v", result, fake.events)
	}
}

type countingHost struct{ calls int }

func (h *countingHost) Run(context.Context, string, io.Writer, io.Writer) Result {
	h.calls++
	return Result{}
}

func TestImageJobNeverUsesHostAndNeverFallsBack(t *testing.T) {
	host := &countingHost{}
	fake := &fakeDocker{inspectErr: errors.New("daemon unavailable")}
	router := Job{Local: host, Docker: &Docker{client: fake, directory: t.TempDir()}}
	result := router.RunJob(context.Background(), "docker", imageJob("image", "true"), io.Discard, io.Discard)
	if host.calls != 0 || result.ExitCode != -1 || result.Err == nil {
		t.Fatalf("host calls=%d result=%+v", host.calls, result)
	}
}

func TestDockerCancellationKillsAndRemovesContainer(t *testing.T) {
	fake := &fakeDocker{blockOutput: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Result, 1)
	go func() {
		done <- (&Docker{client: fake, directory: t.TempDir()}).RunJob(ctx, "test", imageJob("image", "wait"), io.Discard, io.Discard)
	}()
	for {
		fake.mu.Lock()
		attached := false
		for _, event := range fake.events {
			if event == "exec-attach" {
				attached = true
			}
		}
		fake.mu.Unlock()
		if attached {
			break
		}
	}
	cancel()
	result := <-done
	if !errors.Is(result.Err, context.Canceled) || fake.kills != 1 || fake.removes != 1 {
		t.Fatalf("result=%+v kills=%d removes=%d events=%v", result, fake.kills, fake.removes, fake.events)
	}
}
