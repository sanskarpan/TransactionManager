#!/bin/bash
A=$(curl -s -X POST localhost:8080/api/txn/begin -H 'Content-Type: application/json' -d '{"protocol":"2pl","isolation":"serializable"}' | jq -r .id)
B=$(curl -s -X POST localhost:8080/api/txn/begin -H 'Content-Type: application/json' -d '{"protocol":"2pl","isolation":"serializable"}' | jq -r .id)
curl -s -X POST localhost:8080/api/txn/$A/write -H 'Content-Type: application/json' -d '{"table":"accounts","key":"10","values":[{"Type":1,"Int":10},{"Type":3,"Text":"owner-10"},{"Type":2,"Float":8000},{"Type":3,"Text":"central"}]}' >/dev/null
curl -s -X POST localhost:8080/api/txn/$B/write -H 'Content-Type: application/json' -d '{"table":"accounts","key":"20","values":[{"Type":1,"Int":20},{"Type":3,"Text":"owner-20"},{"Type":2,"Float":7000},{"Type":3,"Text":"central"}]}' >/dev/null
curl -s -X POST localhost:8080/api/txn/$A/write -H 'Content-Type: application/json' -d '{"table":"accounts","key":"20","values":[{"Type":1,"Int":20},{"Type":3,"Text":"owner-20"},{"Type":2,"Float":6000},{"Type":3,"Text":"central"}]}' >/dev/null &
curl -s -X POST localhost:8080/api/txn/$B/write -H 'Content-Type: application/json' -d '{"table":"accounts","key":"10","values":[{"Type":1,"Int":10},{"Type":3,"Text":"owner-10"},{"Type":2,"Float":5000},{"Type":3,"Text":"central"}]}' >/dev/null &
sleep 2
