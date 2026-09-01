package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/executor"
	"github.com/nhatminh06/forgeci/internal/pipeline"
	"github.com/nhatminh06/forgeci/internal/runner"
)

const helpText = `ForgeCI executes repository-local pipelines.

Usage:
  forge run [--file <path>] [--jobs <N>]
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
	if args[0] != "run" {
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], helpText)
		return 2
	}
	return run(ctx, args[1:], directory, stdout, stderr)
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
	result := (runner.Runner{Executor: local, Output: stdout, ErrorOutput: stderr, MaxParallel: *jobs}).Run(ctx, graph)
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
