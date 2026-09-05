#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d /tmp/forgeci-m12-mutations.XXXXXX)
pg="forgeci-m12-mutations-$$"
killed=0

cleanup() {
  if [[ -d "$tmp/backups" ]]; then
    while IFS= read -r file; do cp "$tmp/backups/$file" "$root/$file"; done <"$tmp/files"
  fi
  docker rm -f "$pg" >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

files=(internal/api/api.go internal/scm/scm.go internal/scm/git/materialize.go internal/scm/github/checks.go internal/store/postgres/scm_deliveries.go internal/store/postgres/scm_triggers.go)
mkdir -p "$tmp/backups"
: >"$tmp/files"
for file in "${files[@]}"; do mkdir -p "$tmp/backups/$(dirname "$file")"; cp "$root/$file" "$tmp/backups/$file"; printf '%s\n' "$file" >>"$tmp/files"; done

docker run -d --name "$pg" -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for _ in $(seq 1 100); do docker exec "$pg" pg_isready -U postgres -d forgeci >/dev/null 2>&1 && break; sleep .1; done
port=$(docker port "$pg" 5432/tcp | sed 's/.*://')
export TEST_DATABASE_URL="postgres://postgres:forgeci@127.0.0.1:$port/forgeci?sslmode=disable"
export GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache}

restore_file() { cp "$tmp/backups/$1" "$root/$1"; }
kill_mutation() {
  id=$1 file=$2 test_cmd=$3
  shift 3
  restore_file "$file"
  perl -0pi -e "$*" "$root/$file"
  if (cd "$root" && eval "$test_cmd") >"$tmp/$id-kill.log" 2>&1; then
    echo "mutation $id survived"; cat "$tmp/$id-kill.log"; exit 1
  fi
  restore_file "$file"
  (cd "$root" && eval "$test_cmd") >"$tmp/$id-restore.log" 2>&1
  killed=$((killed+1))
  echo "$id PASS/killed"
}

kill_mutation A internal/api/api.go "go test ./internal/api -run TestGitHubWebhookValidationAndRegistrationGate -count=1" 's/if err := githubscm\.VerifySignature\(s\.GitHubWebhookSecret, r\.Header\.Get\("X-Hub-Signature-256"\), body\); err != nil \{/if false {/'
kill_mutation B internal/store/postgres/scm_deliveries.go "go test ./internal/store/postgres -run TestSCMDeliveryConcurrentConflict -count=1" 's/if out\.PayloadSHA256 != in\.PayloadSHA256 \{/if false {/'
kill_mutation C internal/scm/scm.go "go test ./internal/scm/git -run TestRemoteTrustedIdentity -count=1" 's/\\\\@/\\\\/'
kill_mutation D internal/scm/git/materialize.go "go test ./internal/scm/git -run TestPrepareExactBranchRevision -count=1" 's/"checkout", "--detach", in\.CommitSHA/"checkout", "--detach", ref/'
restore_file internal/scm/git/materialize.go
perl -0pi -e 's/"checkout", "--detach", in\.CommitSHA/"checkout", "--detach", "FETCH_HEAD"/; s/err != nil \|\| !strings\.EqualFold\(strings\.TrimSpace\(head\), in\.CommitSHA\)/err != nil/' "$root/internal/scm/git/materialize.go"
if (cd "$root" && go test ./internal/scm/git -run TestPrepareExactBranchRevision -count=1) >"$tmp/E-kill.log" 2>&1; then echo "mutation E survived"; exit 1; fi
restore_file internal/scm/git/materialize.go
(cd "$root" && go test ./internal/scm/git -run TestPrepareExactBranchRevision -count=1) >"$tmp/E-restore.log" 2>&1
killed=$((killed+1)); echo "E PASS/killed"
kill_mutation F internal/scm/git/materialize.go "go test ./internal/scm/git -run TestPrepareKeepsTokenOutOfArgvRemoteAndConfigAndCleansResources -count=1" 's/"origin", remote/"origin", remote+in.Token/'
kill_mutation G internal/store/postgres/scm_deliveries.go "go test ./internal/store/postgres -run TestSCMDeliveryClaimHasOneOwnerAndCountsOneAttempt -count=1" 's/\(status='"'"'PROCESSING'"'"' AND claim_expires_at <= \$1\)/(status='"'"'PROCESSING'"'"')/'
kill_mutation H internal/store/postgres/scm_deliveries.go "go test ./internal/store/postgres -run TestSCMDeliveryClaimRejectsWrongAndExpiredOwners -count=1" 's/AND claim_token=\$2 AND claim_expires_at>now\(\)/AND \$2=\$2 AND claim_expires_at>now\(\)/'
kill_mutation I internal/store/postgres/scm_triggers.go "go test ./internal/store/postgres -run TestSCMRunCreationAndTriggerAreAtomicAndIdempotent -count=1" 's/if err := createRun\(ctx, tx, runIn\); err != nil \{\n\t\treturn nil, nil, err\n\t\}/if err := createRun(ctx, tx, runIn); err != nil { return nil, nil, err }; if err := tx.Commit(ctx); err != nil { return nil, nil, err }/'
kill_mutation J internal/store/postgres/scm_deliveries.go "go test ./internal/store/postgres -run TestSCMDeliveryClaimRejectsWrongAndExpiredOwners -count=1" 's/\n\t\t   OR \(status='"'"'PROCESSING'"'"' AND claim_expires_at <= \$1\)//'
kill_mutation K internal/scm/github/checks.go "go test ./internal/scm/github -run TestReconcileCheckAdoptsLostCreateResponseAndUpdatesIdempotently -count=1" 's/if check\.ExternalID == in\.ExternalID \{/if false {/'
kill_mutation L internal/store/postgres/scm_deliveries.go "go test ./internal/store/postgres -run TestSCMPullRequestSupersessionCancelsOlderWorkAndRejectsStaleWorker -count=1" 's/if in\.EventType == string\(scm\.EventPullRequest\)/if false \&\& in.EventType == string(scm.EventPullRequest)/'

[[ "$killed" == 12 ]]
echo "M12 mutation matrix: 12/12 killed; 12/12 restored tests pass"
