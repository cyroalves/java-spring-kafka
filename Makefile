# Java 21+ é obrigatório (Spring Boot 4). O JAVA_HOME do ambiente costuma
# apontar para um JDK antigo, então ele é escolhido por detecção — e `:=` (e não
# `?=`) para que a detecção ganhe da variável de ambiente. Passar
# `make JAVA_HOME=/caminho` ainda sobrescreve: argumento de linha de comando tem
# prioridade sobre qualquer atribuição no Makefile.
JAVA_HOME := $(shell ./scripts/find-jdk.sh)
MVN       := JAVA_HOME=$(JAVA_HOME) ./mvnw
API       ?= http://localhost:8090
BROKER    ?= java-spring-kafka-kafka-1

.PHONY: help check-java up down logs ui run build test verify \
        order burst flaky boom invalid rollback \
        topics lag dlt peek reset clean

help: ## lista os alvos
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

check-java: ## mostra qual JDK será usado no build
	@echo "JAVA_HOME = $(JAVA_HOME)"
	@$(JAVA_HOME)/bin/java -version 2>&1 | head -1

## ---------- infra ----------
up: ## sobe o broker (KRaft) e espera ficar saudável
	docker compose up -d --wait

down: ## derruba o broker e apaga os dados
	docker compose down -v

logs: ## acompanha o log do broker
	docker compose logs -f kafka

ui: ## sobe a UI web em http://localhost:8091
	docker compose --profile ui up -d --wait

## ---------- aplicação ----------
build: check-java ## compila e empacota (sem testes)
	$(MVN) -DskipTests package

run: check-java ## sobe a aplicação em http://localhost:8090
	$(MVN) spring-boot:run

test: check-java ## testes de integração contra um Kafka real (Testcontainers)
	$(MVN) test

verify: check-java ## build completo com testes
	$(MVN) verify

clean: ## limpa o target
	$(MVN) clean

## ---------- demos (com a aplicação no ar) ----------
order: ## publica um pedido válido -> vira fatura
	@curl -s -X POST $(API)/api/orders -H 'Content-Type: application/json' \
		-d '{"customerId":"customer-ok","amount":250.00}'; echo

burst: ## publica 9 pedidos entre 3 clientes -> mostra particionamento
	@curl -s -X POST "$(API)/api/orders/burst?count=9"; echo

flaky: ## falha temporária que se resolve no 3º retry
	@curl -s -X POST $(API)/api/orders -H 'Content-Type: application/json' \
		-d '{"customerId":"flaky-1","amount":300.00}'; echo

boom: ## falha temporária que nunca passa -> esgota o backoff e cai na DLT
	@curl -s -X POST $(API)/api/orders -H 'Content-Type: application/json' \
		-d '{"customerId":"boom-1","amount":400.00}'; echo

invalid: ## erro permanente -> DLT direto, sem retry
	@curl -s -X POST $(API)/api/orders -H 'Content-Type: application/json' \
		-d '{"customerId":"customer-big","amount":99999.00}'; echo

rollback: ## dispara o rollback da transação de faturamento
	@curl -s -X POST $(API)/api/orders -H 'Content-Type: application/json' \
		-d '{"customerId":"customer-rollback","amount":6666.00}'; echo

## ---------- inspeção ----------
topics: ## lista os tópicos e suas partições
	@docker exec $(BROKER) /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:19092 --describe

lag: ## mostra o lag de cada grupo de consumo
	@docker exec $(BROKER) /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:19092 --all-groups --describe

dlt: ## lê a dead letter queue
	@docker exec $(BROKER) /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:19092 --topic orders.v1.DLT \
		--from-beginning --timeout-ms 5000 2>/dev/null || true

peek: ## compara read_committed x read_uncommitted em invoices.v1
	./scripts/peek-isolation.sh

reset: ## apaga os dados do broker e sobe de novo (zera offsets e tópicos)
	docker compose down -v && docker compose up -d --wait
