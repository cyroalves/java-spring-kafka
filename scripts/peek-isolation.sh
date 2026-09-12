#!/usr/bin/env bash
# Mostra a diferença entre os dois níveis de isolamento no mesmo tópico.
#
# Rode `make rollback` antes: ele produz faturas que são escritas no log e
# depois abortadas. read_uncommitted enxerga todas; read_committed só as que
# pertencem a transações efetivamente commitadas.
set -euo pipefail

BROKER="${BROKER:-java-spring-kafka-kafka-1}"
TOPIC="${TOPIC:-invoices.v1}"

consume() {
  docker exec "$BROKER" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:19092 \
    --topic "$TOPIC" --from-beginning \
    --isolation-level "$1" --timeout-ms 6000 2>/dev/null || true
}

echo "=== read_uncommitted: tudo o que está fisicamente no log ==="
consume read_uncommitted | tee /tmp/kafka-uncommitted.$$ | sed 's/^/  /'
echo
echo "=== read_committed: o que um consumidor correto enxerga ==="
consume read_committed | tee /tmp/kafka-committed.$$ | sed 's/^/  /'
echo
printf '=== resumo: %s registros gravados, %s visíveis, %s descartados por rollback ===\n' \
  "$(wc -l < /tmp/kafka-uncommitted.$$)" \
  "$(wc -l < /tmp/kafka-committed.$$)" \
  "$(( $(wc -l < /tmp/kafka-uncommitted.$$) - $(wc -l < /tmp/kafka-committed.$$) ))"
rm -f /tmp/kafka-uncommitted.$$ /tmp/kafka-committed.$$
