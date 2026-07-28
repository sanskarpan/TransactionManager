Output docs/screenshots/demo-api.gif

Require curl
Require jq

Set Shell "bash"
Set FontSize 14
Set Width 900
Set Height 520
Set Theme "Catppuccin Mocha"
Set TypingSpeed 35ms
Set Padding 20

Type "# MVCC Snapshot Isolation — T2 sees a frozen snapshot"
Enter
Sleep 400ms

Type `T1=$(curl -s -X POST localhost:8080/api/txn/begin \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"protocol":"mvcc","isolation":"serializable"}' | jq -r .id)`
Enter
Sleep 600ms

Type `T2=$(curl -s -X POST localhost:8080/api/txn/begin \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"protocol":"mvcc","isolation":"serializable"}' | jq -r .id)`
Enter
Sleep 600ms

Type `echo "T1=$T1  T2=$T2"`
Enter
Sleep 400ms

Type "# T1 updates account 10: balance 10000 → 9000"
Enter
Type `curl -s -X POST localhost:8080/api/txn/$T1/write \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"10","values":[{"Type":1,"Int":10},{"Type":3,"Text":"owner-10"},{"Type":2,"Float":9000},{"Type":3,"Text":"central"}]}' \`
Enter
Type `  | jq .`
Enter
Sleep 900ms

Type "# T2 reads account 10 — sees 10000, not 9000 (snapshot isolation!)"
Enter
Type `curl -s -X POST localhost:8080/api/txn/$T2/read \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"10"}' \`
Enter
Type `  | jq '{found:.found, balance:.values[2].Float}'`
Enter
Sleep 900ms

Type "# T1 commits"
Enter
Type `curl -s -X POST localhost:8080/api/txn/$T1/commit | jq .`
Enter
Sleep 600ms

Type "# T2 reads again AFTER T1 committed — still 10000 (snapshot preserved!)"
Enter
Type `curl -s -X POST localhost:8080/api/txn/$T2/read \`
Enter
Type `  -H 'Content-Type: application/json' \`
Enter
Type `  -d '{"table":"accounts","key":"10"}' \`
Enter
Type `  | jq '{found:.found, balance:.values[2].Float}'`
Enter
Sleep 900ms

Type `curl -s -X POST localhost:8080/api/txn/$T2/commit | jq .`
Enter
Sleep 1200ms
