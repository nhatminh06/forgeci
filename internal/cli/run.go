package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	controlclient "github.com/nhatminh06/forgeci/internal/client"
	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/executor"
	"github.com/nhatminh06/forgeci/internal/pipeline"
	"github.com/nhatminh06/forgeci/internal/runner"
)

const helpText = `ForgeCI executes repository-local pipelines.

Usage:
  forge run [--file <path>] [--jobs <N>]
  forge submit [--server <url>] [--file <path>] [--jobs <N>]
  forge runs [--server <url>] [--limit <N>]
  forge inspect <run-id> [--server <url>]
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
	case "inspect":
		return inspect(ctx, args[1:], stdout, stderr)
	case "cancel":
		return cancelRun(ctx, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], helpText)
		return 2
	}
}

const defaultServer = "http://127.0.0.1:8080"

func submit(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	server := flags.String("server", defaultServer, "control-plane URL")
	file := flags.String("file", "forge.yaml", "pipeline file relative to server workspace")
	jobs := flags.Int("jobs", 1, "maximum concurrent jobs")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *jobs < 1 {
		fmt.Fprintln(stderr, "invalid submit arguments")
		return 2
	}
	id, err := controlclient.New(*server).Submit(ctx, *file, *jobs)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fmt.Fprintf(stdout, "Run %s queued\n", id)
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
	items, err := controlclient.New(*server).Runs(ctx, *limit)
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
	item, err := controlclient.New(*server).Inspect(ctx, id)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	fmt.Fprintf(stdout, "Run\nID: %s\nStatus: %s\nPipeline: %s\nCreated: %s\n", item.ID, item.Status, item.PipelineFile, item.CreatedAt.Format(time.RFC3339))
	if item.StartedAt != nil {
		fmt.Fprintf(stdout, "Started: %s\n", item.StartedAt.Format(time.RFC3339))
	}
	if item.FinishedAt != nil {
		fmt.Fprintf(stdout, "Finished: %s\n", item.FinishedAt.Format(time.RFC3339))
	}
	fmt.Fprintln(stdout, "\nJobs\nNAME\tSTATUS")
	for _, job := range item.Jobs {
		fmt.Fprintf(stdout, "%s\t%s\n", job.Name, job.Status)
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
	if err := controlclient.New(*server).Cancel(ctx, id); err != nil {
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
	result := (runner.Runner{Executor: jobExecutor, Output: stdout, ErrorOutput: stderr, MaxParallel: *jobs}).Run(ctx, graph)
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
