#!/usr/bin/env bash
# Kafka CLI only. Run against a NEW disposable broker: topic transactions must not exist.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p evidence
BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
GROUP="lab2-$(date +%s)"
PIDS=()
cleanup() { for pid in "${PIDS[@]}"; do pkill -TERM -P "$pid" 2>/dev/null || true; kill "$pid" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT
cli() {
  local tool=$1; shift
  if [[ -n ${KAFKA_BIN:-} ]]; then
    "$KAFKA_BIN/kafka-${tool}.sh" "$@"
  else
    docker compose exec -T kafka "kafka-${tool}" "$@"
  fi
}
{
  printf 'Execution mode: %s\n' "${BROKER_MODE:-Docker Compose / Confluent 7.5.0}"
  printf 'Broker: %s; group prefix: %s\n' "$BOOTSTRAP" "$GROUP"
  set -x
  cli broker-api-versions --bootstrap-server "$BOOTSTRAP"
  cli topics --bootstrap-server "$BOOTSTRAP" --create --topic transactions --partitions 1 --replication-factor 1
  cli topics --bootstrap-server "$BOOTSTRAP" --describe --topic transactions
  printf 'first\nsecond\nthird\n' | cli console-producer --bootstrap-server "$BOOTSTRAP" --topic transactions
  cli console-consumer --bootstrap-server "$BOOTSTRAP" --topic transactions --group "$GROUP-resume" \
    --from-beginning --max-messages 3 --consumer-property auto.commit.interval.ms=100 \
    --consumer-property max.poll.records=1 --property print.partition=true --property print.offset=true \
    >evidence/first3.txt
  cli consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$GROUP-resume"
  printf 'fourth\nfifth\n' | cli console-producer --bootstrap-server "$BOOTSTRAP" --topic transactions
  cli console-consumer --bootstrap-server "$BOOTSTRAP" --topic transactions --group "$GROUP-resume" \
    --from-beginning --max-messages 2 --consumer-property max.poll.records=1 \
    --property print.partition=true --property print.offset=true >evidence/resume2.txt
  cli console-consumer --bootstrap-server "$BOOTSTRAP" --topic transactions --group "$GROUP-new" \
    --from-beginning --max-messages 5 --property print.partition=true --property print.offset=true \
    >evidence/newgroup5.txt
  cli consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$GROUP-resume"
  cli consumer-groups --bootstrap-server "$BOOTSTRAP" --group "$GROUP-resume" --topic transactions \
    --reset-offsets --to-earliest --execute
  cli console-consumer --bootstrap-server "$BOOTSTRAP" --topic transactions --group "$GROUP-resume" \
    --from-beginning --max-messages 5 --property print.partition=true --property print.offset=true \
    >evidence/reset5.txt
  cli topics --bootstrap-server "$BOOTSTRAP" --alter --topic transactions --partitions 3
  cli topics --bootstrap-server "$BOOTSTRAP" --describe --topic transactions
  for i in 1 2; do
    cli console-consumer --bootstrap-server "$BOOTSTRAP" --topic transactions --group "$GROUP-two" \
      --consumer-property client.id="two-$i" --from-beginning --timeout-ms 45000 \
      --property print.partition=true --property print.offset=true >"evidence/two-$i.txt" 2>"evidence/two-$i.err" &
    PIDS+=("$!")
  done
  sleep 8
  cli consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$GROUP-two" --members --verbose \
    | tee evidence/two-members.txt
  for i in 1 2 3 4; do
    cli console-consumer --bootstrap-server "$BOOTSTRAP" --topic transactions --group "$GROUP-four" \
      --consumer-property client.id="four-$i" --from-beginning --timeout-ms 45000 \
      --property print.partition=true --property print.offset=true >"evidence/four-$i.txt" 2>"evidence/four-$i.err" &
    PIDS+=("$!")
  done
  sleep 10
  for i in $(seq 1 12); do printf 'event-%s\n' "$i"; done \
    | cli console-producer --bootstrap-server "$BOOTSTRAP" --topic transactions
  sleep 2
  cli consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$GROUP-four" --members --verbose \
    | tee evidence/four-members.txt
  cli consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$GROUP-four"
  set +x
} >evidence/commands.log 2>&1
# Assertions describe the actual formatter output rather than hypothetical observations.
[[ $(wc -l <evidence/first3.txt) -eq 3 ]]
[[ $(wc -l <evidence/resume2.txt) -eq 2 ]]
[[ $(wc -l <evidence/newgroup5.txt) -eq 5 ]]
[[ $(wc -l <evidence/reset5.txt) -eq 5 ]]
grep -q 'Offset:3' evidence/resume2.txt
grep -q 'Offset:4' evidence/resume2.txt
grep -q 'Offset:0' evidence/reset5.txt
[[ $(grep -c 'two-' evidence/two-members.txt) -eq 2 ]]
[[ $(grep -c 'four-' evidence/four-members.txt) -eq 4 ]]
grep -Eq 'four-[0-9]+[[:space:]]+0([[:space:]]+-)?[[:space:]]*$' evidence/four-members.txt
printf 'All CLI assertions passed. Read evidence/commands.log and REPORT.md.\n' | tee evidence/result.txt
