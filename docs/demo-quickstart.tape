Output docs/screenshots/demo-quickstart.gif

Require curl
Require jq

Set Shell "bash"
Set FontSize 14
Set Width 900
Set Height 420
Set Theme "Catppuccin Mocha"
Set TypingSpeed 35ms
Set Padding 20

Type "# Quick start — server is already running on :8080"
Enter
Sleep 400ms

Type `curl -s localhost:8080/healthz | jq .`
Enter
Sleep 700ms

Type `curl -s localhost:8080/readyz | jq .`
Enter
Sleep 500ms

Type "# Begin a serializable 2PL transaction"
Enter

Type `TXN=$(curl -s -X POST localhost:8080/api/txn/begin \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"protocol":"2pl","isolation":"serializable","lockTimeoutMs":5000}' | jq -r .id)`
Enter
Sleep 700ms
Type `echo "txn_id=$TXN"`
Enter
Sleep 400ms

Type "# Read → write → commit"
Enter

Type `curl -s -X POST localhost:8080/api/txn/$TXN/read \`
Enter
Type `  -d '{"table":"accounts","key":"1"}' \`
Enter
Type `  -H 'Content-Type: application/json' | jq .`
Enter
Sleep 800ms

Type `curl -s -X POST localhost:8080/api/txn/$TXN/write \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"1","values":[{"type":"int","int_val":9800}]}' | jq .`
Enter
Sleep 800ms

Type `curl -s -X POST localhost:8080/api/txn/$TXN/commit | jq .`
Enter
Sleep 700ms

Type "# Live metrics"
Enter
Type `curl -s localhost:8080/api/metrics | jq '{commits:.txn_commits,begins:.txn_begins}'`
Enter
Sleep 1000ms
