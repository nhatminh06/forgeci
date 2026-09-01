# CLAUDE.md

# ForgeCI Engineering Guide

ForgeCI is a self-hosted CI/CD platform built from first principles.

The project exists to learn and implement the internals of:

* pipeline engines
* DAG scheduling
* local and remote runners
* build execution
* artifact and cache systems
* CI observability
* software supply-chain controls
* deployment orchestration
* failure recovery
* distributed CI infrastructure

The project should evolve incrementally.

Do not prematurely turn ForgeCI into a large platform before the underlying mechanisms are understood and verified.

---

# 1. Core Engineering Principles

Follow these principles for every milestone.

## Correctness before features

Prefer:

```text
small
correct
tested
understood
```

over:

```text
large
abstract
feature-rich
unfinished
```

Do not implement future milestones simply because the architecture could support them.

---

## Build from first principles

ForgeCI should implement and expose the important CI/CD mechanisms itself where doing so is educational.

Avoid replacing core project problems with large external platforms.

For example:

Good:

```text
ForgeCI parses a DAG
ForgeCI schedules jobs
ForgeCI tracks job state
ForgeCI manages runners
```

Avoid prematurely delegating these responsibilities to:

```text
GitHub Actions
Tekton
Argo Workflows
Jenkins
```

External tools may eventually be integrated where appropriate, but they should not replace the systems ForgeCI is intended to teach.

---

## Keep milestone scope strict

Every milestone has a clearly defined scope.

Do not implement functionality assigned to future milestones unless required to make the current milestone correct.

If additional work appears necessary:

1. determine whether it is a correctness requirement
2. implement the smallest required fix
3. document why it was needed

Do not expand scope opportunistically.

---

# 2. Current Project Direction

ForgeCI should evolve approximately through these areas:

```text
Pipeline parsing
      ↓
DAG compilation
      ↓
Local execution
      ↓
Parallel scheduling
      ↓
Container execution
      ↓
Control plane
      ↓
Remote runners
      ↓
Artifacts and caching
      ↓
SCM integration
      ↓
Kubernetes runners
      ↓
Secrets and security
      ↓
Deployment orchestration
      ↓
Observability
      ↓
High availability
      ↓
Self-hosting
```

This is directional only.

The active milestone specification always takes precedence.

---

# 3. Primary Language

ForgeCI's main implementation language is:

```text
Go
```

Write idiomatic Go.

Prefer:

* simple packages
* explicit data structures
* clear ownership
* useful errors
* standard library functionality when appropriate
* small dependency surface

Avoid:

* excessive interfaces
* speculative abstractions
* deep inheritance-style designs
* dependency injection frameworks
* generic frameworks built before they are needed
* unnecessary reflection
* global mutable state

Interfaces should represent real boundaries, not exist merely for architectural appearance.

---

# 4. Architecture Rules

Maintain clear separation between major stages.

A pipeline should conceptually move through:

```text
configuration
      ↓
parsing
      ↓
validation
      ↓
compilation
      ↓
execution graph
      ↓
runtime state
      ↓
execution
```

Do not let later execution code repeatedly interpret raw YAML.

Configuration models and runtime models should remain distinct when practical.

Avoid circular package dependencies.

Prefer dependency direction from higher-level orchestration toward lower-level mechanisms.

---

# 5. Repository Structure

Use structure only when implementation requires it.

A likely long-term shape is:

```text
cmd/
  forge/

internal/
  config/
  pipeline/
  scheduler/
  executor/
  runner/
  artifact/
  cache/
  scm/
  telemetry/

docs/

testdata/
```

Do not create empty directories or placeholder packages simply to match this layout.

The repository should reflect implemented functionality, not future architecture diagrams.

---

# 6. Error Handling

Normal user or pipeline errors must not panic.

Examples include:

```text
invalid YAML
unknown dependency
dependency cycle
missing pipeline file
command failure
bad CLI arguments
```

Return useful contextual errors.

Prefer:

```text
job "test" depends on unknown job "build"
```

over:

```text
invalid pipeline
```

Use panics only for genuine programmer invariants where recovery is inappropriate.

---

# 7. Determinism

CI systems must behave predictably.

Whenever multiple equivalent choices exist, use deterministic behavior.

Examples:

```text
job ordering
graph traversal
summary output
test fixtures
serialization
```

Never rely on Go map iteration order for observable behavior.

If multiple runnable jobs require deterministic ordering, use an explicit policy such as lexicographic job-name ordering unless the active milestone specifies otherwise.

---

# 8. Pipeline Semantics

Pipeline semantics must be explicit and tested.

Important behaviors include:

```text
dependency resolution
cycle detection
job readiness
job failure
dependent-job blocking
independent-job continuation
pipeline result
```

Do not rely on accidental behavior.

Whenever semantics change, update:

```text
implementation
tests
documentation
examples
```

together.

---

# 9. Execution Safety

Treat execution boundaries carefully.

Early ForgeCI milestones may execute trusted commands directly on the local machine.

If so, document clearly that there is:

```text
no sandbox
no container isolation
no tenant isolation
no secret isolation
```

Do not describe local shell execution as secure isolation.

Future container or remote-runner milestones must define their threat model explicitly.

---

# 10. Testing Philosophy

Tests must prove behavior rather than merely increase coverage.

Prefer tests that would fail if the intended mechanism were removed.

Test:

```text
success paths
failure paths
boundary conditions
invalid state
dependency semantics
deterministic behavior
```

Avoid tests coupled excessively to private implementation details.

Use table-driven tests where they improve clarity.

Use temporary directories and deterministic local resources.

Unit tests should not require:

```text
internet access
GitHub
Docker
Kubernetes
root access
```

unless the milestone specifically introduces those dependencies.

---

# 11. Mutation Proofs

When a milestone asks for mutation evidence, perform it.

The pattern is:

1. add the correct test
2. temporarily disable the target guard or behavior
3. run the focused test
4. prove the test fails for the intended reason
5. restore the implementation
6. rerun the test successfully
7. never commit the mutation

Examples:

```text
disable cycle rejection
allow blocked dependency execution
remove bounds validation
disable retry limit
```

Do not fabricate mutation evidence.

---

# 12. Verification

Before finalizing milestone work, run all relevant verification.

For Go code, normally include:

```bash
gofmt
go vet ./...
go test ./...
go test -race ./...
```

Build the real binary when applicable.

Run actual CLI scenarios in addition to unit tests.

Check:

```bash
git diff --check
git status
```

Do not report tests as passing unless they were actually run.

---

# 13. Documentation

Documentation must describe what ForgeCI actually implements.

Do not describe roadmap capabilities as current functionality.

Keep these distinctions explicit:

```text
implemented
planned
experimental
unsupported
```

Architecture documentation should explain why components exist and how data moves through the system.

Avoid documentation that simply repeats source code.

---

# 14. README Philosophy

The root README should remain portfolio-readable.

It should explain:

```text
what ForgeCI is
why it exists
current capabilities
architecture
how to run it
what the current milestone demonstrates
limitations
roadmap
```

Do not turn the README into a chronological engineering diary.

Deep implementation notes belong under:

```text
docs/
```

---

# 15. Git Rules

Use professional feature branches.

Recommended pattern:

```text
milestone/01-local-pipeline-engine
milestone/02-parallel-scheduler
milestone/03-docker-executor
```

Use clear commits such as:

```text
feat(config): validate pipeline definitions
feat(pipeline): compile dependency graph
feat(executor): execute local jobs
test(pipeline): cover dependency failures
docs: document local pipeline execution
```

Do not:

```text
force-push
rewrite shared history
create meaningless checkpoint commits
mix unrelated refactors into milestone work
```

Do not push directly to the primary branch when milestone work is intended for a pull request.

---

# 16. No AI Attribution

Never add:

```text
Claude
Anthropic
ChatGPT
OpenAI
Codex
AI-generated
generated by AI
```

to:

```text
source code
comments
documentation
commit messages
pull requests
Git metadata
```

Never add AI `Co-Authored-By` trailers.

Do not mention the coding agent in repository artifacts.

---

# 17. Dependency Policy

Keep third-party dependencies minimal.

Before adding a dependency, determine whether:

```text
the standard library solves the problem cleanly
```

If an external dependency is justified:

* use a mature maintained library
* keep its responsibility narrow
* avoid framework-sized dependencies for small problems

Examples of reasonable dependencies may include:

```text
YAML parsing
CLI parsing when CLI complexity justifies it
database drivers once persistent storage exists
```

Do not add infrastructure dependencies before their milestone.

---

# 18. Generated Files

Do not commit:

```text
compiled binaries
temporary test output
coverage files
local databases
runtime logs
build directories
IDE metadata
```

unless explicitly required.

Keep `.gitignore` accurate.

---

# 19. Refactoring

Refactor when it improves a real problem.

Good reasons:

```text
duplicated logic
unclear ownership
unmaintainable file size
testing difficulty
incorrect dependency direction
```

Bad reason:

```text
this abstraction might be useful six milestones later
```

Prefer the smallest architecture that cleanly supports the current milestone.

---

# 20. Comments

Comments should explain:

```text
why
invariant
protocol constraint
non-obvious failure case
```

Avoid comments that merely translate code into English.

Do not add excessive explanatory commentary inside straightforward functions.

---

# 21. Performance

Do not optimize prematurely.

However, avoid obviously pathological designs.

When performance becomes a milestone goal:

1. establish measurement
2. identify the bottleneck
3. change implementation
4. measure again

Do not claim performance improvements without evidence.

---

# 22. Concurrency

Concurrency must be introduced deliberately.

Before parallel execution exists, prefer simple sequential correctness.

Once concurrency is introduced:

* define ownership
* define cancellation behavior
* define synchronization
* define deterministic observable semantics where possible
* run the Go race detector
* test failure interactions

Do not add goroutines merely because Go makes them easy.

---

# 23. Future Distributed-System Rules

When ForgeCI later introduces remote runners and control-plane coordination, explicitly reason about:

```text
job leases
heartbeats
runner loss
duplicate execution
idempotency
retries
durability
recovery
scheduler ownership
```

Never assume reliable networks or reliable workers.

These concerns should not be implemented before their milestone.

---

# 24. Security

Security features must fail closed where appropriate.

Examples:

```text
invalid signatures
unknown identities
bad secrets
untrusted artifacts
authorization failures
```

Do not silently weaken validation to make tests pass.

Never log secret values.

Do not claim a security property that has not been implemented and tested.

---

# 25. Scope Discipline

At the start and end of every milestone, explicitly identify:

```text
what belongs in this milestone
what remains deferred
```

If work introduces unrelated systems, remove or defer them.

A successful ForgeCI milestone should leave the codebase:

```text
working
tested
understandable
documented
ready for the next layer
```

not merely larger.

---

# 26. Engineering Reports

After completing a milestone, provide a short educational explanation of what was built.

Explain mechanisms, not only changed filenames.

Where useful, include commands the project owner can run manually to observe:

```text
successful behavior
failure behavior
boundary behavior
dependency behavior
recovery behavior
```

The goal is for each milestone to improve both the project and the owner's understanding of the system.

---

# 27. Current Priority

Always follow the active milestone prompt.

Do not begin the next milestone until the current one is:

```text
implemented
tested
reviewed
documented
committed
pushed
```

and, when the workflow requires it:

```text
merged
```

The current milestone specification overrides roadmap assumptions in this file.

---

# 28. Final Principle

ForgeCI should grow by adding one understandable systems concept at a time.

Prefer:

```text
understand → implement → test → break → verify → document → merge
```

before:

```text
add more features
```
