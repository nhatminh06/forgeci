#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d /tmp/forgeci-github-scm.XXXXXX)
pg="forgeci-m12-postgres-$$"
api_port=$((41000 + ($$ % 500)))
runner_port=$((42000 + ($$ % 500)))
github_port=$((43000 + ($$ % 500)))
server_pid=""
runner_pid=""
github_pid=""

cleanup() {
  for pid in "$runner_pid" "$server_pid" "$github_pid"; do
    [[ -z "$pid" ]] || kill -TERM "$pid" >/dev/null 2>&1 || true
  done
  for pid in "$runner_pid" "$server_pid" "$github_pid"; do
    [[ -z "$pid" ]] || wait "$pid" >/dev/null 2>&1 || true
  done
  docker rm -f "$pg" >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

mkdir -p "$tmp/bin" "$tmp/git/owner" "$tmp/work" "$tmp/server-work" "$tmp/snapshots" "$tmp/artifacts" "$tmp/runner" "$tmp/runner-state"
printf '%s\n' integration-runner-token >"$tmp/runner-token"
printf '%s\n' integration-webhook-secret >"$tmp/webhook-secret"
chmod 600 "$tmp/runner-token" "$tmp/webhook-secret"
openssl genrsa -out "$tmp/app.pem" 2048 >/dev/null 2>&1
chmod 600 "$tmp/app.pem"

git init --bare "$tmp/git/owner/repo.git" >/dev/null
git init "$tmp/work" >/dev/null
git -C "$tmp/work" config user.email test@example.invalid
git -C "$tmp/work" config user.name test
cat >"$tmp/work/forge.yaml" <<'YAML'
version: 1
jobs:
  exact:
    steps:
      - run: test "$(cat marker.txt)" = A
YAML
printf A >"$tmp/work/marker.txt"
git -C "$tmp/work" add .
git -C "$tmp/work" commit -m A >/dev/null
sha_a=$(git -C "$tmp/work" rev-parse HEAD)
git -C "$tmp/work" remote add origin "$tmp/git/owner/repo.git"
git -C "$tmp/work" push origin HEAD:refs/heads/main >/dev/null
git -C "$tmp/work" push origin "$sha_a:refs/pull/1/head" >/dev/null
printf B >"$tmp/work/marker.txt"
sed -i 's/= A/= B/' "$tmp/work/forge.yaml"
git -C "$tmp/work" add .
git -C "$tmp/work" commit -m B >/dev/null
sha_b=$(git -C "$tmp/work" rev-parse HEAD)
git -C "$tmp/work" push origin HEAD:refs/heads/main >/dev/null
git --git-dir="$tmp/git/owner/repo.git" update-server-info

cat >"$tmp/fake_github.py" <<'PY'
import http.server, json, os, re, sys, threading
root, state_path, fail_token, fail_checks = sys.argv[1:5]
checks = []
lock = threading.Lock()
class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs): super().__init__(*args, directory=root, **kwargs)
    def log_message(self, *_): pass
    def send_json(self, status, value):
        data=json.dumps(value).encode(); self.send_response(status); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(data))); self.end_headers(); self.wfile.write(data)
    def read_json(self):
        return json.loads(self.rfile.read(int(self.headers.get('Content-Length','0'))) or b'{}')
    def do_GET(self):
        if self.path.startswith('/repos/') and '/commits/' in self.path and '/check-runs' in self.path:
            with lock: value={'check_runs': list(checks)}
            return self.send_json(200, value)
        if self.path == '/state':
            with lock: value=list(checks)
            return self.send_json(200, value)
        return super().do_GET()
    def do_POST(self):
        if re.fullmatch(r'/app/installations/[0-9]+/access_tokens', self.path):
            if os.path.exists(fail_token): return self.send_json(503, {'message':'temporary'})
            return self.send_json(201, {'token':'local-installation-token','expires_at':'2035-01-01T00:00:00Z'})
        if self.path.endswith('/check-runs'):
            if os.path.exists(fail_checks): return self.send_json(503, {'message':'temporary'})
            body=self.read_json()
            with lock:
                item={'id':len(checks)+1,'external_id':body['external_id'],'head_sha':body['head_sha'],'status':body['status'],'conclusion':body.get('conclusion')}; checks.append(item)
                open(state_path,'w').write(json.dumps(checks))
            return self.send_json(201,item)
        return self.send_json(404,{})
    def do_PATCH(self):
        if os.path.exists(fail_checks): return self.send_json(503, {'message':'temporary'})
        body=self.read_json(); ident=int(self.path.rsplit('/',1)[1])
        with lock:
            for item in checks:
                if item['id']==ident: item.update(status=body['status'],conclusion=body.get('conclusion'))
            open(state_path,'w').write(json.dumps(checks))
        return self.send_json(200,{})
http.server.ThreadingHTTPServer(('127.0.0.1', int(os.environ['PORT'])), Handler).serve_forever()
PY
PORT="$github_port" python3 "$tmp/fake_github.py" "$tmp/git" "$tmp/checks.json" "$tmp/fail-token" "$tmp/fail-checks" >"$tmp/github.log" 2>&1 &
github_pid=$!

GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$tmp/bin/forge" "$root/cmd/forge"
GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$tmp/bin/forge-server" "$root/cmd/forge-server"
GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$tmp/bin/forge-runner" "$root/cmd/forge-runner"

docker run -d --name "$pg" -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for _ in $(seq 1 100); do docker exec "$pg" pg_isready -U postgres -d forgeci >/dev/null 2>&1 && break; sleep .1; done
db_port=$(docker port "$pg" 5432/tcp | sed 's/.*://')
for _ in $(seq 1 100); do docker exec "$pg" psql -U postgres -d forgeci -At -c 'SELECT 1' >/dev/null 2>&1 && break; sleep .1; done
for _ in $(seq 1 100); do timeout 1 bash -c "</dev/tcp/127.0.0.1/$db_port" >/dev/null 2>&1 && break; sleep .1; done
database_url="postgres://postgres:forgeci@127.0.0.1:$db_port/forgeci?sslmode=disable"

start_server() {
  mode=$1
  args=(--execution-mode remote --listen "127.0.0.1:$api_port" --runner-listen "127.0.0.1:$runner_port" --runner-token-file "$tmp/runner-token" --workspace "$tmp/server-work" --snapshot-dir "$tmp/snapshots" --artifact-dir "$tmp/artifacts" --database-url "$database_url" --github-webhook-secret-file "$tmp/webhook-secret" --github-clone-base-url "http://127.0.0.1:$github_port" --scm-worker-concurrency 4 --scm-worker-lease 10s)
  if [[ "$mode" == app ]]; then args+=(--github-app-id 42 --github-private-key-file "$tmp/app.pem" --github-api-base-url "http://127.0.0.1:$github_port"); fi
  "$tmp/bin/forge-server" "${args[@]}" >"$tmp/server.log" 2>&1 & server_pid=$!
  for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$api_port/healthz" >/dev/null 2>&1 && return; kill -0 "$server_pid" 2>/dev/null || break; sleep .1; done
  cat "$tmp/server.log"; exit 1
}
stop_server() { [[ -z "$server_pid" ]] || { kill -TERM "$server_pid"; wait "$server_pid" || true; server_pid=""; }; }
sql() { docker exec "$pg" psql -U postgres -d forgeci -At -c "$1"; }
wait_sql() { query=$1; wanted=$2; for _ in $(seq 1 300); do [[ "$(sql "$query")" == "$wanted" ]] && return; sleep .1; done; echo "timeout: $query"; sql "$query"; exit 1; }
payload_push() { printf '{"ref":"refs/heads/main","after":"%s","installation":{"id":7},"repository":{"full_name":"owner/repo"}}' "$1"; }
send_hook() {
  event=$1 delivery=$2 body=$3
  signature=$(BODY="$body" python3 -c 'import hashlib,hmac,os; print("sha256="+hmac.new(b"integration-webhook-secret",os.environ["BODY"].encode(),hashlib.sha256).hexdigest())')
  curl -sS -o "$tmp/response" -w '%{http_code}' -H "X-GitHub-Event: $event" -H "X-GitHub-Delivery: $delivery" -H "X-Hub-Signature-256: $signature" --data-binary "$body" "http://127.0.0.1:$api_port/v1/hooks/github"
}
wait_delivery() { wait_sql "SELECT status FROM scm_deliveries WHERE delivery_id='$1'" "$2"; }
run_for() { sql "SELECT t.run_id FROM scm_run_triggers t JOIN scm_deliveries d ON d.id=t.delivery_id WHERE d.delivery_id='$1'"; }
wait_run() { wait_sql "SELECT p.status FROM pipeline_runs p JOIN scm_run_triggers t ON t.run_id=p.id JOIN scm_deliveries d ON d.id=t.delivery_id WHERE d.delivery_id='$1'" "$2"; }
wait_check() { wait_sql "SELECT COALESCE(t.check_state,'')||'/'||COALESCE(t.last_check_conclusion,'') FROM scm_run_triggers t JOIN scm_deliveries d ON d.id=t.delivery_id WHERE d.delivery_id='$1'" "$2"; }

start_server app
"$tmp/bin/forge" repo add github owner/repo --server "http://127.0.0.1:$api_port" >/dev/null
FORGECI_RUNNER_TOKEN=integration-runner-token "$tmp/bin/forge-runner" --server "http://127.0.0.1:$runner_port" --workspace-root "$tmp/runner" --state-dir "$tmp/runner-state" --name github-runner --max-parallel 2 >"$tmp/runner.log" 2>&1 & runner_pid=$!

body_a=$(payload_push "$sha_a")
[[ "$(send_hook push push-a "$body_a")" == 202 ]]
wait_delivery push-a PROCESSED; wait_run push-a PASSED; wait_check push-a completed/success
[[ "$(sql "SELECT t.commit_sha FROM scm_run_triggers t JOIN scm_deliveries d ON d.id=t.delivery_id WHERE d.delivery_id='push-a'")" == "$sha_a" ]]

body_b=$(payload_push "$sha_b")
[[ "$(send_hook push push-b "$body_b")" == 202 ]]
[[ "$(send_hook push push-b "$body_b")" == 202 ]]
wait_run push-b PASSED; wait_check push-b completed/success
[[ "$(sql "SELECT count(*) FROM scm_deliveries WHERE delivery_id='push-b'")" == 1 ]]
[[ "$(sql "SELECT count(*) FROM scm_run_triggers t JOIN scm_deliveries d ON d.id=t.delivery_id WHERE d.delivery_id='push-b'")" == 1 ]]
[[ "$(send_hook push push-b "${body_b%?},\"x\":1}")" == 409 ]]

pr_a=$(printf '{"action":"opened","installation":{"id":7},"repository":{"full_name":"owner/repo"},"pull_request":{"number":1,"draft":false,"head":{"sha":"%s","ref":"topic"},"base":{"ref":"main"}}}' "$sha_a")
[[ "$(send_hook pull_request pr-a "$pr_a")" == 202 ]]
wait_run pr-a PASSED; wait_check pr-a completed/success
[[ "$(sql "SELECT t.commit_sha FROM scm_run_triggers t JOIN scm_deliveries d ON d.id=t.delivery_id WHERE d.delivery_id='pr-a'")" == "$sha_a" ]]

stop_server
start_server noapp
[[ "$(send_hook push restart "$body_b")" == 202 ]]
wait_delivery restart PENDING
stop_server; start_server app
wait_run restart PASSED; wait_check restart completed/success

stop_server; start_server noapp
[[ "$(send_hook push lease "$body_b")" == 202 ]]
sql "UPDATE scm_deliveries SET status='PROCESSING',claim_token=gen_random_uuid(),claimed_by='dead',claim_expires_at=now()-interval '1 second',attempt_count=1 WHERE delivery_id='lease'" >/dev/null
stop_server; start_server app
wait_run lease PASSED
[[ "$(sql "SELECT attempt_count FROM scm_deliveries WHERE delivery_id='lease'")" == 2 ]]

stop_server
touch "$tmp/fail-token"
start_server app
[[ "$(send_hook push token-retry "$body_b")" == 202 ]]
wait_delivery token-retry FAILED
rm "$tmp/fail-token"
wait_run token-retry PASSED

touch "$tmp/fail-checks"
[[ "$(send_hook push check-restart "$body_b")" == 202 ]]
wait_run check-restart PASSED
wait_sql "SELECT CASE WHEN last_check_error IS NULL THEN 'no' ELSE 'yes' END FROM scm_run_triggers t JOIN scm_deliveries d ON d.id=t.delivery_id WHERE d.delivery_id='check-restart'" yes
stop_server; rm "$tmp/fail-checks"; start_server app
wait_check check-restart completed/success

cat >"$tmp/work/forge.yaml" <<'YAML'
version: 1
jobs:
  fail:
    steps:
      - run: exit 7
YAML
git -C "$tmp/work" add forge.yaml
git -C "$tmp/work" commit -m failure >/dev/null
sha_fail=$(git -C "$tmp/work" rev-parse HEAD)
git -C "$tmp/work" push origin HEAD:refs/heads/main >/dev/null
git --git-dir="$tmp/git/owner/repo.git" update-server-info
body_fail=$(payload_push "$sha_fail")
[[ "$(send_hook push failure "$body_fail")" == 202 ]]
wait_run failure FAILED; wait_check failure completed/failure

[[ "$(sql 'SELECT count(*) FROM pipeline_runs')" == "$(sql 'SELECT count(*) FROM scm_run_triggers')" ]]
[[ -z "$(find /tmp -maxdepth 1 -name 'forgeci-scm-*' -print)" ]]
printf 'GitHub SCM integration passed\n'
