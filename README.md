# java-spring-kafka

Pipeline de pedidos orientado a eventos com **Spring Boot 4.1** e **Apache Kafka 4**,
escrito para expor — e explicar — as decisões que normalmente ficam implícitas:
onde a ordem é garantida, o que acontece quando um consumidor falha, e até onde
"exactly-once" realmente vai.

Não é um "hello world" de `@KafkaListener`. Cada garantia do sistema está
implementada, demonstrável por um comando, e coberta por teste de integração
contra um broker real.

---

## O fluxo

```
                      POST /api/orders
                             │
                             ▼
                   ┌───────────────────┐
                   │  OrderPublisher   │  produtor idempotente, acks=all
                   │  key = customerId │  (ordem preservada por cliente)
                   └─────────┬─────────┘
                             ▼
                   ┌───────────────────┐
                   │  orders.v1        │  3 partições
                   └────┬─────────┬────┘
                        │         │        dois grupos, o log inteiro para cada um
        grupo           │         │           grupo
    order-validation    │         │         invoicing
                        ▼         ▼
        ┌───────────────────┐   ┌────────────────────────┐
        │ OrderValidation   │   │ InvoiceProcessor       │
        │ Listener          │   │ (read-process-write)   │
        │                   │   │                        │
        │ retry + backoff   │   │ transação:             │
        │ classifica erro   │   │  send + offset commit  │
        └────────┬──────────┘   └───────────┬────────────┘
                 │ esgotou                  │
                 ▼                          ▼
        ┌───────────────────┐   ┌────────────────────────┐
        │ orders.v1.DLT     │   │ invoices.v1            │
        │ + headers de causa│   │ (lido em               │
        └────────┬──────────┘   │  read_committed)       │
                 ▼              └───────────┬────────────┘
        ┌───────────────────┐               ▼
        │ DeadLetterListener│   ┌────────────────────────┐
        │ (ack manual)      │   │ InvoiceAuditListener   │
        └───────────────────┘   └────────────────────────┘
```

Os dois consumidores leem **o mesmo tópico em grupos diferentes** — cada grupo
recebe todas as mensagens e mantém o próprio offset. É a diferença entre um log
particionado e uma fila.

---

## Rodando

Requisitos: Docker e um JDK 21+ (o `make` detecta qual usar; Spring Boot 4 não
roda em Java 17 ou anterior).

```bash
make up       # sobe o broker Kafka (KRaft, sem Zookeeper) na porta 39092
make run      # sobe a aplicação em http://localhost:8090
make ui       # opcional: UI web do cluster em http://localhost:8091
```

Com a aplicação no ar, cada alvo demonstra um comportamento:

| comando         | o que demonstra                                                     |
|-----------------|---------------------------------------------------------------------|
| `make order`    | caminho feliz: pedido → validação → fatura                          |
| `make burst`    | particionamento por chave: 9 pedidos, 3 clientes, 3 partições        |
| `make flaky`    | falha temporária que se resolve no 3º retry                          |
| `make boom`     | falha temporária que nunca passa: esgota o backoff e cai na DLT      |
| `make invalid`  | erro permanente: vai para a DLT sem gastar retry                     |
| `make rollback` | transação abortada — a fatura é escrita no log e fica invisível      |
| `make lag`      | lag por grupo e partição                                             |
| `make dlt`      | conteúdo da dead letter queue                                        |
| `make peek`     | compara `read_committed` e `read_uncommitted` no mesmo tópico        |

```bash
make verify   # build completo + testes de integração (Testcontainers)
make down     # derruba o broker e apaga os dados
```

---

## O que está implementado, e por quê

### Ordem
A chave da mensagem é o **`customerId`**, não o `orderId`. Kafka só garante ordem
dentro de uma partição, e a partição sai de `hash(chave)`. Chaveando por cliente,
os pedidos de um mesmo cliente ficam na mesma partição e são processados na ordem
em que chegaram. Com `orderId` — único por mensagem — cada pedido cairia em uma
partição qualquer e a ordem por cliente se perderia.

### Entrega
Produtor com `acks=all` e `enable.idempotence=true`: o broker deduplica reenvios
de rede, então um retry não vira mensagem duplicada. Consumidor com
`ack-mode=RECORD` — o offset avança depois do processamento, o que dá
**at-least-once**. Não existe at-most-once *e* at-least-once ao mesmo tempo; a
escolha aqui é duplicar em vez de perder, e o consumidor precisa ser idempotente.

### Falhas
`DefaultErrorHandler` com backoff exponencial (500ms → 1s → 2s → 4s) e
classificação de exceções:

- `InvalidOrderException` e `DeserializationException` são **permanentes** — vão
  direto para a DLT, sem gastar tentativa. Repetir dado inválido só trava a
  partição.
- Qualquer outra exceção é tratada como **temporária** e passa pelo backoff.

Enquanto o backoff corre, a partição inteira fica parada: o container faz seek de
volta ao offset que falhou. Por isso o teto é 4s, e não 30s — backoff generoso em
consumidor Kafka é indisponibilidade disfarçada de resiliência.

### Dead letter queue
O `DeadLetterPublishingRecoverer` republica o registro em `orders.v1.DLT`,
preservando chave, payload e partição de origem, e anexando headers `kafka_dlt-*`
com tópico/partição/offset originais, classe e mensagem da exceção e stacktrace.
Um consumidor dedicado lê a DLT com **ack manual** — o offset só avança depois do
tratamento.

O destino é declarado explicitamente em vez de usar o sufixo padrão do framework:
esse padrão mudou entre as linhas 3.x e 4.x do spring-kafka, e um destino
implícito errado não dá erro — o produtor apenas trava em
`UNKNOWN_TOPIC_OR_PARTITION`.

### Transações (read-process-write)
O `InvoiceProcessor` roda em um container com `KafkaTransactionManager`: a fatura
publicada e o commit do offset do pedido entram **na mesma transação**. Sem isso
há dois buracos — publicar a fatura e morrer antes de commitar o offset (fatura
duplicada) ou commitar o offset e morrer antes de publicar (pedido sem fatura).

Os consumidores rodam em `isolation.level=read_committed`, e é o que dá efeito
prático ao rollback: `make rollback` faz o processador publicar a fatura e então
falhar. A fatura **é escrita fisicamente no log** e continua lá — só fica
invisível. `make peek` mostra os dois lados:

```
=== resumo: 14 registros gravados, 10 visíveis, 4 descartados por rollback ===
```

Duas honestidades sobre esse "exactly-once":

1. **Ele vale dentro do Kafka.** Não se estende a um banco externo nem a uma
   chamada HTTP. Para esses, o caminho é outbox transacional + idempotência.
2. **A transação de recuperação commita junto.** Quando as tentativas se esgotam
   e o registro vai para a DLT com `commitRecovered=true`, o envio da dead letter
   e o offset commitam juntos — e levam junto o que o listener já tinha produzido
   na última tentativa. Efeito colateral que não pode sobreviver a uma falha deve
   ser produzido no fim do método, nunca antes da parte que pode falhar.

### Contrato das mensagens
O payload não carrega o nome da classe Java. O header `__TypeId__` leva um nome
lógico (`order`, `invoice`) mapeado por `spring.json.type.mapping` dos dois lados,
então produtor e consumidor podem estar em pacotes — ou linguagens — diferentes.
O tópico é versionado no nome (`orders.v1`): mudança incompatível cria `v2` e os
dois convivem durante a migração.

O deserializer é embrulhado em `ErrorHandlingDeserializer`. Sem ele, um JSON
malformado estoura **antes** do listener, onde nenhum error handler alcança, e o
container entra em laço infinito na mesma mensagem.

---

## Testes

`make verify` sobe um broker Kafka real via Testcontainers (`@ServiceConnection`)
e exercita os três caminhos que definem o projeto:

```
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
```

- pedido válido percorre o pipeline e vira fatura `committed`;
- pedido acima do limite vai direto para a DLT, e os headers `kafka_dlt-*` trazem
  a causa correta;
- falha temporária se recupera no retry e **não** gera dead letter.

Broker real em vez de mock por um motivo simples: transação, rebalance,
`read_committed` e o comportamento da DLT não existem em mock. Os testes leem o
payload como string crua — verificam o que chegou no tópico, não o que a própria
aplicação conseguiria desserializar.

---

## Detalhe operacional que vale conhecer

Um consumidor de tópico escrito transacionalmente mostra **lag 1 permanente** em
cada partição, mesmo tendo consumido tudo:

```
GROUP           TOPIC         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
invoice-audit   invoices.v1   0          15              16              1
invoice-audit   invoices.v1   1          5               6               1
invoice-audit   invoices.v1   2          5               6               1
```

Os marcadores de commit/abort da transação ocupam um offset, mas nunca são
entregues ao consumidor. Alerta configurado em `lag > 0` dispara para sempre.

---

## Estrutura

```
src/main/java/com/cyro/kafka/
├── config/
│   ├── Topics.java                    nomes e versionamento dos tópicos
│   ├── KafkaTopicConfig.java          criação dos tópicos no start
│   ├── KafkaProducerConfig.java       produtor comum (não transacional)
│   ├── KafkaTransactionConfig.java    produtor transacional + container R-P-W
│   └── KafkaErrorHandlingConfig.java  backoff, classificação, DLT, ack manual
├── order/
│   ├── Order.java                     evento de orders.v1
│   ├── NewOrderRequest.java           contrato HTTP, separado do evento
│   ├── OrderRules.java                regras compartilhadas pelos dois grupos
│   ├── OrderPublisher.java            produtor
│   └── OrderValidationListener.java   consumidor com retry e DLT
├── invoice/
│   ├── Invoice.java
│   ├── InvoiceProcessor.java          read-process-write transacional
│   └── InvoiceAuditListener.java      prova o efeito do read_committed
├── deadletter/
│   └── DeadLetterListener.java        inspeção da DLT com ack manual
└── web/
    └── OrderController.java           API HTTP (responde 202, não 201)
```

## Stack

Spring Boot 4.1.1 · Spring Kafka 4.1.1 · Apache Kafka 4.0 (KRaft) · Java 21 ·
Jackson 3 · Testcontainers · Maven Wrapper
