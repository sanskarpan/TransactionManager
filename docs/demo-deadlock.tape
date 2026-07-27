Output docs/screenshots/demo-deadlock.gif

Require curl
Require jq

Set Shell "bash"
Set FontSize 14
Set Width 900
Set Height 480
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms
Set Padding 20

Type "# Deadlock Detection — 2PL with cycle detection"
Enter
Sleep 400ms

Type "# Begin two 2PL transactions"
Enter

Type `A=$(curl -s -X POST localhost:8080/api/txn/begin \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"protocol":"2pl","isolation":"serializable"}' | jq -r .id)`
Enter
Sleep 600ms

Type `B=$(curl -s -X POST localhost:8080/api/txn/begin \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"protocol":"2pl","isolation":"serializable"}' | jq -r .id)`
Enter
Sleep 600ms

Type `echo "TxA=$A  TxB=$B"`
Enter
Sleep 400ms

Type "# TxA locks row 1, TxB locks row 2"
Enter

Type `curl -s -X POST localhost:8080/api/txn/$A/write \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"1","values":[{"type":"int","int_val":100}]}' | jq .status`
Enter
Sleep 600ms

Type `curl -s -X POST localhost:8080/api/txn/$B/write \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"2","values":[{"type":"int","int_val":200}]}' | jq .status`
Enter
Sleep 600ms

Type "# Now they cross-lock — deadlock! Detector aborts the youngest."
Enter
Sleep 200ms

Type `curl -s -X POST localhost:8080/api/txn/$A/write \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"2","values":[{"type":"int","int_val":150}]}' &`
Enter
Sleep 200ms

Type `curl -s -X POST localhost:8080/api/txn/$B/write \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"1","values":[{"type":"int","int_val":250}]}' &`
Enter
Sleep 2000ms

Type "# Check deadlock history"
Enter
Type `curl -s localhost:8080/api/deadlocks | jq .[0]`
Enter
Sleep 1200ms
