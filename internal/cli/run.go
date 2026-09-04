package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nhatminh06/forgeci/internal/artifact"
	"github.com/nhatminh06/forgeci/internal/cache"
	controlclient "github.com/nhatminh06/forgeci/internal/client"
	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/executor"
	"github.com/nhatminh06/forgeci/internal/pipeline"
	"github.com/nhatminh06/forgeci/internal/runner"
	"github.com/nhatminh06/forgeci/internal/store"
)

const helpText = `ForgeCI executes repository-local pipelines.

Usage:
  forge run [--file <path>] [--jobs <N>]
  forge submit [--server <url>] [--file <path>] [--jobs <N>]
  forge runs [--server <url>] [--limit <N>]
  forge runners [--server <url>]
  forge cache list [--server <url>]
  forge cache delete <key> [--server <url>]
  forge artifacts <run-id> [--server <url>]
  forge artifact download <run-id> <job> <name> --output <path> [--server <url>]
  forge inspect <run-id> [--server <url>]
  forge wait <run-id> [--server <url>] [--timeout <duration>]
  forge logs <run-id> --job <job> [--server <url>]
  forge cancel <run-id> [--server <url>]
  forge --help

Default pipeline file:
  forge.yaml

Options:
  --file <path>  pipeline YAML file (default forge.yaml)
  --jobs <N>     maximum number of concurrently running jobs (default 1)
`

func Main(ctx context.Context, args []string, directory string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, helpText)
		return 2
	}
	if args[0] == "--help" || args[0] == "-h" {
		fmt.Fprint(stdout, helpText)
		return 0
	}
	switch args[0] {
	case "run":
		return run(ctx, args[1:], directory, stdout, stderr)
	case "submit":
		return submit(ctx, args[1:], stdout, stderr)
	case "runs":
		return runs(ctx, args[1:], stdout, stderr)
	case "runners":
		return runners(ctx, args[1:], stdout, stderr)
	case "cache":
		return cacheCommand(ctx, args[1:], stdout, stderr)
	case "inspect":
		return inspect(ctx, args[1:], stdout, stderr)
	case "wait":
		return waitRun(ctx, args[1:], stdout, stderr)
	case "logs":
		return logs(ctx, args[1:], stdout, stderr)
	case "cancel":
		return cancelRun(ctx, args[1:], stdout, stderr)
	case "artifacts":
		return artifacts(ctx, args[1:], stdout, stderr)
	case "artifact":
		return artifactCommand(ctx, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], helpText)
		return 2
	}
}

func waitRun(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "wait requires one run ID")
		return 2
	}
	flags := flag.NewFlagSet("wait", flag.ContinueOnError)
	flags.SetOutput(stderr)
	server := flags.String("server", defaultServer, "control-plane URL")
	timeout := flags.Duration("timeout", 15*time.Minute, "maximum wait duration")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || *timeout <= 0 {
		fmt.Fprintln(stderr, "invalid wait arguments")
		return 2
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client := newControlClient(*server)
	for {
		run, err := client.Inspect(ctx, args[0])
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		if run.FinishedAt != nil {
			fmt.Fprintf(stdout, "Run %s %s\n", run.ID, run.Status)
			for _, job := range run.Jobs {
				fmt.Fprintf(stdout, "%s %s\n", job.Name, job.Status)
			}
			if run.Status == store.RunPassed {
				return 0
			}
			return 1
		}
		select {
		case <-ctx.Done():
			fmt.Fprintln(stderr, ctx.Err())
			return 2
		case <-time.After(waitPollInterval):
		}
	}
}

func logs(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "logs requires a run ID")
		return 2
	}
	flags := flag.NewFlagSet("logs", flag.ContinueOnError)
	flags.SetOutput(stderr)
	job := flags.String("job", "", "job name")
	server := flags.String("server", defaultServer, "control-plane URL")
	follow := flags.Bool("follow", false, "wait for new durable log chunks")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || *job == "" {
		if *job == "" {
			fmt.Fprintln(stderr, "logs requires --job")
		}
		return 2
	}
	client := newControlClient(*server)
	var after int64
	for {
		var items []store.JobLogChunk
		var err error
		if *follow {
			items, err = client.JobLogsFollow(ctx, args[0], *job, after, 256)
		} else {
			items, err = client.JobLogs(ctx, args[0], *job, after, 256)
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		for _, c := range items {
			if _, err := stdout.Write(c.Payload); err != nil {
				return 2
			}
			if c.Sequence > after {
				after = c.Sequence
			}
		}
		if len(items) < 256 {
			if *follow && len(items) == 0 {
				return 0
			}
			if !*follow {
				return 0
			}
		}
		if *follow && len(items) == 0 {
			return 0
		}
	}
}

func cacheCommand(ctx context.Context, args []string, out, errw io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(errw, "usage: forge cache list|delete")
		return 2
	}
	vals, pos, e := parseLooseFlags(args[1:], map[string]string{"--server": defaultServer})
	if e != nil {
		fmt.Fprintln(errw, e)
		return 2
	}
	c := newControlClient(vals["--server"])
	switch args[0] {
	case "list":
		if len(pos) != 0 {
			return 2
		}
		items, e := c.CacheList(ctx, 100)
		if e != nil {
			fmt.Fprintln(errw, e)
			return 2
		}
		for _, v := range items {
			fmt.Fprintf(out, "%s\t%s\t%d\n", v.Key, v.ContentSHA256, v.ArchiveSizeBytes)
		}
		return 0
	case "delete":
		if len(pos) != 1 {
			fmt.Fprintln(errw, "usage: forge cache delete <key>")
			return 2
		}
		if e := c.CacheDelete(ctx, pos[0]); e != nil {
			fmt.Fprintln(errw, e)
			return 2
		}
		return 0
	}
	fmt.Fprintln(errw, "unknown cache command")
	return 2
}

func artifacts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	server, positionals, err := parseLooseFlags(args, map[string]string{"--server": defaultServer})
	if err != nil || len(positionals) != 1 {
		fmt.Fprintln(stderr, "usage: forge artifacts <run-id> [--server <url>]")
		return 2
	}
	items, err := newControlClient(server["--server"]).Artifacts(ctx, positionals[0])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fmt.Fprintln(stdout, "JOB\tNAME\tSIZE\tSTATUS\tEXPIRES")
	for _, item := range items {
		status := "available"
		if !item.Available {
			status = "expired"
		}
		expires := "-"
		if item.ExpiresAt != nil {
			expires = item.ExpiresAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(stdout, "%s\t%s\t%d\t%s\t%s\n", item.ProducerJob, item.Name, item.ArchiveSizeBytes, status, expires)
	}
	return 0
}
func artifactCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "download" {
		fmt.Fprintln(stderr, "usage: forge artifact download <run-id> <job> <name> --output <path> [--server <url>]")
		return 2
	}
	values, positionals, err := parseLooseFlags(args[1:], map[string]string{"--server": defaultServer, "--output": ""})
	if err != nil || len(positionals) != 3 || values["--output"] == "" {
		fmt.Fprintln(stderr, "usage: forge artifact download <run-id> <job> <name> --output <path> [--server <url>]")
		return 2
	}
	f, err := os.OpenFile(values["--output"], os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	digest, downloadErr := newControlClient(values["--server"]).DownloadArtifact(ctx, positionals[0], positionals[1], positionals[2], f)
	closeErr := f.Close()
	if downloadErr != nil || closeErr != nil {
		_ = os.Remove(values["--output"])
		if downloadErr != nil {
			fmt.Fprintln(stderr, downloadErr)
		} else {
			fmt.Fprintln(stderr, closeErr)
		}
		return 2
	}
	fmt.Fprintf(stdout, "Downloaded %s/%s (%s) to %s\n", positionals[1], positionals[2], digest, values["--output"])
	return 0
}
func parseLooseFlags(args []string, defaults map[string]string) (map[string]string, []string, error) {
	values := make(map[string]string, len(defaults))
	for k, v := range defaults {
		values[k] = v
	}
	var positional []string
	for i := 0; i < len(args); i++ {
		if _, ok := values[args[i]]; ok {
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("missing value for %s", args[i])
			}
			values[args[i]] = args[i+1]
			i++
			continue
		}
		if strings.HasPrefix(args[i], "-") {
			return nil, nil, fmt.Errorf("unknown flag %s", args[i])
		}
		positional = append(positional, args[i])
	}
	return values, positional, nil
}

const defaultServer = "http://127.0.0.1:8080"

var newControlClient = controlclient.New
var waitPollInterval = 500 * time.Millisecond

func submit(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	server := flags.String("server", defaultServer, "control-plane URL")
	file := flags.String("file", "forge.yaml", "pipeline file relative to server workspace")
	jobs := flags.Int("jobs", 1, "maximum concurrent jobs")
	quiet := flags.Bool("quiet", false, "print only the run ID")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *jobs < 1 {
		fmt.Fprintln(stderr, "invalid submit arguments")
		return 2
	}
	id, err := newControlClient(*server).Submit(ctx, *file, *jobs)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if *quiet {
		fmt.Fprintln(stdout, id)
	} else {
		fmt.Fprintf(stdout, "Run %s queued\n", id)
	}
	return 0
}
func runs(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("runs", flag.ContinueOnError)
	flags.SetOutput(stderr)
	server := flags.String("server", defaultServer, "control-plane URL")
	limit := flags.Int("limit", 20, "maximum runs")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *limit < 1 || *limit > 100 {
		fmt.Fprintln(stderr, "limit must be between 1 and 100")
		return 2
	}
	items, err := newControlClient(*server).Runs(ctx, *limit)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fmt.Fprintln(stdout, "ID\tSTATUS\tCREATED")
	for _, item := range items {
		fmt.Fprintf(stdout, "%s\t%s\t%s\n", item.ID, item.Status, item.CreatedAt.Format(time.RFC3339))
	}
	return 0
}
func runners(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("runners", flag.ContinueOnError)
	flags.SetOutput(stderr)
	server := flags.String("server", defaultServer, "control-plane URL")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "unexpected arguments")
		return 2
	}
	runners, err := newControlClient(*server).Runners(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fmt.Fprintln(stdout, "NAME\tSTATUS\tOS\tARCH\tDOCKER\tACTIVE\tCAPACITY")
	for _, runner := range runners {
		docker := "no"
		if runner.DockerAvailable {
			docker = "yes"
		}
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%s\t%d\t%d\n",
			runner.Name, runner.Status, runner.OS, runner.Arch, docker, runner.ActiveJobs, runner.MaxParallel)
	}
	return 0
}
func inspect(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "inspect requires one run ID")
		return 2
	}
	id := args[0]
	flags := flag.NewFlagSet("inspect", flag.ContinueOnError)
	flags.SetOutput(stderr)
	server := flags.String("server", defaultServer, "control-plane URL")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
		return 2
	}
	item, err := newControlClient(*server).Inspect(ctx, id)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fmt.Fprintf(stdout, "Run\nID: %s\nStatus: %s\nPipeline: %s\nCreated: %s\n", item.ID, item.Status, item.PipelineFile, item.CreatedAt.Format(time.RFC3339))
	fmt.Fprintf(stdout, "Pipeline SHA-256: %s\n", item.PipelineSHA256)
	if item.SourceSnapshotSHA256 != nil {
		fmt.Fprintf(stdout, "Source snapshot SHA-256: %s\n", *item.SourceSnapshotSHA256)
	}
	if item.StartedAt != nil {
		fmt.Fprintf(stdout, "Started: %s\n", item.StartedAt.Format(time.RFC3339))
	}
	if item.FinishedAt != nil {
		fmt.Fprintf(stdout, "Finished: %s\n", item.FinishedAt.Format(time.RFC3339))
	}
	fmt.Fprintln(stdout, "\nJobs\nNAME\tSTATUS\tRUNNER")
	for _, job := range item.Jobs {
		runnerID := "-"
		if job.RunnerID != nil {
			runnerID = *job.RunnerID
		}
		fmt.Fprintf(stdout, "%s\t%s\t%s\n", job.Name, job.Status, runnerID)
	}
	return 0
}

func cancelRun(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "cancel requires one run ID")
		return 2
	}
	id := args[0]
	flags := flag.NewFlagSet("cancel", flag.ContinueOnError)
	flags.SetOutput(stderr)
	server := flags.String("server", defaultServer, "control-plane URL")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
		return 2
	}
	if err := newControlClient(*server).Cancel(ctx, id); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fmt.Fprintf(stdout, "Cancellation requested for %s\n", id)
	return 0
}

func run(ctx context.Context, args []string, directory string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	file := flags.String("file", "forge.yaml", "pipeline YAML file")
	jobs := flags.Int("jobs", 1, "maximum number of concurrently running jobs")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "ForgeCI executes repository-local pipelines.")
		fmt.Fprintln(flags.Output())
		fmt.Fprintln(flags.Output(), "Usage:")
		fmt.Fprintln(flags.Output(), "  forge run [--file <path>] [--jobs <N>]")
		fmt.Fprintln(flags.Output())
		fmt.Fprintln(flags.Output(), "Default pipeline file:")
		fmt.Fprintln(flags.Output(), "  forge.yaml")
		fmt.Fprintln(flags.Output())
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected argument %q\n", flags.Arg(0))
		return 2
	}
	if *jobs < 1 {
		fmt.Fprintln(stderr, "jobs must be greater than zero")
		return 2
	}

	fmt.Fprintln(stdout, "ForgeCI")
	fmt.Fprintln(stdout)
	fmt.Fprintf(stdout, "Pipeline: %s\n\n", *file)
	cfg, err := config.Load(*file)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	graph, err := pipeline.Compile(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "compile pipeline: %v\n", err)
		return 2
	}
	local := executor.Local{Directory: directory}
	var docker *executor.Docker
	for _, job := range cfg.Jobs {
		if job.Image != nil {
			docker, err = executor.NewDocker(directory)
			if err != nil {
				fmt.Fprintln(stderr, err)
				return 2
			}
			break
		}
	}
	jobExecutor := executor.Job{Local: local, Docker: docker}
	artifactRoot, err := os.MkdirTemp("", "forgeci-direct-artifacts-*")
	if err != nil {
		fmt.Fprintf(stderr, "create temporary artifact store: %v\n", err)
		return 2
	}
	defer os.RemoveAll(artifactRoot)
	artifactStore, err := artifact.Open(filepath.Join(artifactRoot, "store"), artifact.DefaultLimits())
	if err != nil {
		fmt.Fprintf(stderr, "open temporary artifact store: %v\n", err)
		return 2
	}
	artifactSession, err := artifact.NewSession(directory, filepath.Join(artifactRoot, "work"), artifactStore, artifact.DefaultLimits())
	if err != nil {
		fmt.Fprintf(stderr, "create artifact session: %v\n", err)
		return 2
	}
	cacheStore, err := cache.Open(filepath.Join(artifactRoot, "cache"), artifact.DefaultLimits())
	if err != nil {
		fmt.Fprintf(stderr, "open temporary cache store: %v\n", err)
		return 2
	}
	cacheSession, err := cache.NewSession(directory, filepath.Join(artifactRoot, "cache-work"), cache.NewLocalRemote(cacheStore, directory))
	if err != nil {
		fmt.Fprintf(stderr, "create cache session: %v\n", err)
		return 2
	}
	result := (runner.Runner{Executor: jobExecutor, Output: stdout, ErrorOutput: stderr, MaxParallel: *jobs, Artifacts: artifactSession, Cache: cacheSession}).Run(ctx, graph)
	runner.PrintSummary(stdout, graph, result)
	if result.Interrupted {
		fmt.Fprintln(stderr, "pipeline interrupted")
		return 2
	}
	if result.InternalError {
		fmt.Fprintln(stderr, "pipeline execution could not start")
		return 2
	}
	if !result.Succeeded() {
		return 1
	}
	return 0
}

func WorkingDirectory() (string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("determine working directory: %w", err)
	}
	return directory, nil
}
