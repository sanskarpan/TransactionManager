Output docs/screenshots/demo-deadlock.gif

Require curl
Require jq

Set Shell "bash"
Set FontSize 14
Set Width 1000
Set Height 480
Set Theme "Catppuccin Mocha"
Set TypingSpeed 35ms
Set Padding 20

Type "# 2PL Deadlock Detection — TxA and TxB held cross-locks"
Enter
Sleep 400ms

Type "# Setup: TxA locked row-10, TxB locked row-20. Then they swapped."
Enter
Type "# The wait-for graph formed a cycle — detector aborted the youngest."
Enter
Sleep 800ms

Type `curl -s localhost:8080/api/deadlocks | jq '.[0] | {victim, victimReason, cycle}'`
Enter
Sleep 1500ms

Type `curl -s localhost:8080/api/locks | jq '[.[] | select(.resource | startswith("row:"))] | map({resource, holders: [.granted[].txnId]})'`
Enter
Sleep 1200ms

Type "# Active transactions (TxA still holds its row-10 lock)"
Enter
Type `curl -s localhost:8080/api/txn/active | jq '.[] | {id:.id, protocol:.protocol, status:.status}'`
Enter
Sleep 1200ms
