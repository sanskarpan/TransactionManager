Output docs/screenshots/demo-quickstart.gif

Require curl
Require jq

Set Shell "bash"
Set FontSize 14
Set Width 900
Set Height 480
Set Theme "Catppuccin Mocha"
Set TypingSpeed 35ms
Set Padding 20

Type "# Quick start — server running on :8080"
Enter
Sleep 300ms

Type `curl -s localhost:8080/healthz | jq .`
Enter
Sleep 700ms

Type "# Begin a serializable MVCC transaction"
Enter

Type `TXN=$(curl -s -X POST localhost:8080/api/txn/begin \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"protocol":"mvcc","isolation":"serializable"}' | jq -r .id)`
Enter
Sleep 700ms
Type `echo "txn=$TXN"`
Enter
Sleep 400ms

Type "# Scan accounts table (top 3 rows)"
Enter
Type `curl -s -X POST localhost:8080/api/txn/$TXN/scan \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","limit":3}' \`
Enter
Type `  | jq '[.rows[:3][] | {id:.Values[0].Int, owner:.Values[1].Text, balance:.Values[2].Float}]'`
Enter
Sleep 1000ms

Type `curl -s -X POST localhost:8080/api/txn/$TXN/commit | jq .`
Enter
Sleep 600ms

Type "# Live metrics"
Enter
Type `curl -s localhost:8080/api/metrics | jq '{commits:.txn_commits,begins:.txn_begins}'`
Enter
Sleep 1000ms
