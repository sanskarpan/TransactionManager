Output docs/screenshots/demo-api.gif

Require curl
Require jq

Set Shell "bash"
Set FontSize 14
Set Width 900
Set Height 500
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms
Set Padding 20

Type "# Transaction Manager — live API demo"
Sleep 500ms
Enter
Sleep 300ms

Type "# 1. Begin two concurrent MVCC transactions"
Enter
Sleep 200ms

Type `T1=$(curl -s -X POST localhost:8080/api/txn/begin \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"protocol":"mvcc","isolation":"serializable"}' | jq -r .id)`
Enter
Sleep 800ms

Type `T2=$(curl -s -X POST localhost:8080/api/txn/begin \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"protocol":"mvcc","isolation":"serializable"}' | jq -r .id)`
Enter
Sleep 800ms

Type `echo "T1=$T1  T2=$T2"`
Enter
Sleep 600ms

Type "# 2. T1 writes account balance"
Enter
Sleep 200ms

Type `curl -s -X POST localhost:8080/api/txn/$T1/write \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"1","values":[{"type":"int","int_val":9500}]}' | jq .`
Enter
Sleep 1000ms

Type "# 3. T2 reads the same row (sees old value — snapshot isolation)"
Enter
Sleep 200ms

Type `curl -s -X POST localhost:8080/api/txn/$T2/read \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"1"}' | jq .`
Enter
Sleep 1000ms

Type "# 4. T1 commits, then T2 reads again (still sees snapshot)"
Enter
Sleep 200ms

Type `curl -s -X POST localhost:8080/api/txn/$T1/commit | jq .`
Enter
Sleep 600ms

Type `curl -s -X POST localhost:8080/api/txn/$T2/read \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"1"}' | jq .`
Enter
Sleep 1000ms

Type `curl -s -X POST localhost:8080/api/txn/$T2/commit | jq .`
Enter
Sleep 600ms

Type "# Done — two serializable MVCC transactions, zero conflicts"
Enter
Sleep 1200ms
