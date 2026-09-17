# java-spring-kafka

Pipeline de pedidos orientado a eventos com **Spring Boot 4.1**, **Apache Kafka 4**
e **Postgres**, escrito para expor — e explicar — as decisões que normalmente ficam
implícitas: onde a ordem é garantida, o que acontece quando um consumidor falha,
até onde "exactly-once" realmente vai, e o que sobra para o banco resolver.

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

Um terceiro e um quarto grupo projetam esses mesmos eventos em tabelas:

```
  orders.v1 ──── grupo order-store ────▶ OrderStoreListener ───┐
                                                               │   ┌────────────┐
                                                               ├──▶│  Postgres  │
                                                               │   │  orders    │
invoices.v1 ──── grupo invoice-store ──▶ InvoiceStoreListener ─┘   │  invoices  │
                                                                   └──────┬─────┘
                                                                          ▼
                                                             GET /api/orders/{id}
                                                             GET /api/customers/{id}
```

O banco **não é a fonte da verdade** — o log do Kafka é. As tabelas são projeções
descartáveis: apagar e resetar o offset do grupo reconstrói tudo relendo o tópico.

---

## Rodando

Requisitos: Docker e um JDK 21+ (o `make` detecta qual usar; Spring Boot 4 não
roda em Java 17 ou anterior).

```bash
make up       # sobe Kafka (KRaft, porta 39092) e Postgres (porta 35432)
make run      # sobe a aplicação em http://localhost:8090
make ui       # opcional: UI web do cluster em http://localhost:8091
```

A aplicação exige os dois no ar: sem Postgres, o Flyway falha no start.

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
| `make rows`     | o que os consumidores projetaram no Postgres                         |
| `make orders-api` | os últimos pedidos gravados, pela API de leitura                    |
| `make customer` | resumo de um cliente: soma aprovada, pedidos e faturas                |
| `make psql`     | shell SQL no banco                                                   |

```bash
make verify   # build completo + testes de integração (Testcontainers)
make down     # derruba broker e banco, e apaga os dados dos dois
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

   Com o read model isso deixou de ser sutileza de log e virou linha de tabela:
   `make rollback` escreve 5 faturas no log, 4 ficam invisíveis, **1 chega ao
   Postgres**. Um teste fixa esse número.

### Read model (Postgres, JPA e Hibernate)
Dois consumidores adicionais projetam os eventos em `orders` e `invoices`. A
motivação não é guardar o evento de novo — é responder o que um tópico
particionado não responde: "este pedido específico", "quanto este cliente tem
aprovado". Em Kafka isso seria reler a partição a cada pergunta; em SQL é um
`sum` com índice.

O que sustenta a escolha:

- **Nenhuma configuração torna atômicos o commit do banco e o commit do offset.**
  São duas transações, e o Kafka não faz two-phase commit com o Postgres. A saída
  não é evitar a duplicata — é torná-la inofensiva, com `UNIQUE (order_id)` e
  `UNIQUE (invoice_id)`. O `exists` antes do insert é otimização; **quem garante é
  a constraint**, e a violação é tratada como "já processado" em vez de erro.
- **Schema é do Flyway, não do Hibernate.** `ddl-auto: validate` compara e falha
  se divergir. `update` nunca remove nem renomeia, só acumula, e cada ambiente
  termina com um schema diferente.
- **Id por `SEQUENCE`, não `IDENTITY`.** `IDENTITY` obriga um `INSERT` por
  `persist()` para ler a chave gerada, e o batch nunca se forma. O
  `allocationSize` do `@SequenceGenerator` precisa bater com o `INCREMENT BY` da
  migration — divergir dá violação de chave primária só sob concorrência.
- **Sem chave estrangeira entre `invoices` e `orders`.** Os dois grupos de consumo
  são independentes e não há ordem garantida entre eles: a fatura pode ser gravada
  antes do pedido. Uma FK transformaria essa corrida normal em erro de
  integridade. A integridade aqui é eventual, e o índice assume isso.
- **Três classes para "pedido".** `NewOrderRequest` (corpo do POST), `Order`
  (payload de `orders.v1`) e `OrderRecord` (linha da tabela) mudam por motivos
  diferentes. Uma classe só faria uma anotação JPA vazar para o payload do Kafka.
- **`open-in-view: false`.** Ligado, um getter lazy tocado pelo serializador
  dispara `SELECT` em silêncio. Desligado, vira exceção no desenvolvimento em vez
  de N+1 em produção.

A leitura é eventualmente consistente por construção: um `GET` logo depois do
`POST` pode devolver 404, porque o 202 significa "o Kafka aceitou", não "o
consumidor já gravou".

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

`make verify` sobe um Kafka **e um Postgres** reais via Testcontainers
(`@ServiceConnection`) e exercita os caminhos que definem o projeto:

```
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
```

- pedido válido percorre o pipeline, vira fatura `committed` e aparece nas duas
  tabelas;
- pedido acima do limite vai direto para a DLT com os headers `kafka_dlt-*`
  corretos, e ainda assim é gravado — como `REJECTED`, sem fatura;
- falha temporária se recupera no retry, **não** gera dead letter, e as três
  tentativas viram uma linha só;
- transação abortada: das 5 faturas escritas no log, 4 ficam invisíveis em
  `read_committed` e a 5ª commita junto com a recuperação;
- a API de leitura devolve o pedido com suas faturas, 404 para id ainda não
  projetado e a agregação por cliente.

Serviço real em vez de mock por um motivo simples: transação, rebalance,
`read_committed` e o comportamento da DLT não existem em mock — e `TIMESTAMPTZ`,
sequence e violação de constraint não se comportam igual em banco em memória. Um
teste que passa contra H2 e falha contra Postgres testou o H2.

Os testes leem o payload como string crua — verificam o que chegou no tópico, não
o que a própria aplicação conseguiria desserializar. Os asserts de banco esperam
com Awaitility: a gravação é assíncrona, e assert logo depois do 202 testaria a
velocidade da máquina.

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
│   ├── OrderRules.java                regras compartilhadas pelos grupos
│   ├── OrderPublisher.java            produtor
│   └── OrderValidationListener.java   consumidor com retry e DLT
├── invoice/
│   ├── Invoice.java
│   ├── InvoiceProcessor.java          read-process-write transacional
│   └── InvoiceAuditListener.java      prova o efeito do read_committed
├── store/
│   ├── OrderRecord.java               entidade JPA de orders (só inserção)
│   ├── InvoiceRecord.java             entidade JPA de invoices
│   ├── OrderStatus.java               enum gravado como texto, com CHECK
│   ├── OrderRecordRepository.java     Spring Data + um JPQL de agregação
│   ├── InvoiceRecordRepository.java
│   ├── ReadModelStore.java            limite da transação e idempotência
│   ├── OrderStoreListener.java        grupo order-store
│   └── InvoiceStoreListener.java      grupo invoice-store
├── deadletter/
│   └── DeadLetterListener.java        inspeção da DLT com ack manual
└── web/
    ├── OrderController.java           escrita: publica e responde 202
    ├── ReadModelController.java       leitura: consulta as projeções
    ├── OrderView.java                 DTOs — a entidade não vira JSON
    ├── InvoiceView.java
    ├── OrderDetailView.java
    └── CustomerSummaryView.java

src/main/resources/
├── application.yml                    Kafka, datasource, JPA e Flyway
└── db/migration/
    └── V1__pedidos_e_faturas.sql      tabelas, sequences, índices, constraints
```

## Stack

Spring Boot 4.1.1 · Spring Kafka 4.1.1 · Apache Kafka 4.0 (KRaft) · Java 21 ·
Jackson 3 · Spring Data JPA · Hibernate 7.4 · PostgreSQL 17 · Flyway 12 ·
Testcontainers · Maven Wrapper
