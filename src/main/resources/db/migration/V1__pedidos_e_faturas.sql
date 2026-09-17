-- ---------------------------------------------------------------------------
-- Estado inicial do read model.
--
-- Nenhuma destas tabelas é a fonte da verdade: a fonte é o log do Kafka. Aqui
-- ficam projeções construídas por consumidores, para responder consulta por id
-- e por cliente — coisa que um tópico particionado não faz.
--
-- Migration é imutável: depois de aplicada, o Flyway guarda o checksum em
-- flyway_schema_history e recusa subir se o arquivo mudar. Correção é sempre um
-- V2 novo, nunca uma edição aqui.
-- ---------------------------------------------------------------------------

-- O incremento precisa ser igual ao allocationSize do @SequenceGenerator (50).
-- Se divergirem, o Hibernate distribui ids que a sequence ainda vai devolver e
-- o erro só aparece como violação de chave primária sob concorrência.
CREATE SEQUENCE order_record_seq START WITH 1 INCREMENT BY 50;

CREATE TABLE orders (
    id          BIGINT         NOT NULL,
    -- Chave natural vinda do evento. UNIQUE não é enfeite: é ele que torna o
    -- consumidor idempotente. A entrega do Kafka é at-least-once, então a mesma
    -- mensagem volta em rebalance ou retry, e o banco recusa a segunda gravação.
    order_id    VARCHAR(36)    NOT NULL,
    customer_id VARCHAR(64)    NOT NULL,
    amount      NUMERIC(19, 2) NOT NULL,
    status      VARCHAR(16)    NOT NULL,
    -- created_at é do evento (quando a API aceitou); recorded_at é de quem
    -- gravou. A diferença entre os dois é o lag do consumidor, e some se houver
    -- só uma coluna de "data".
    created_at  TIMESTAMPTZ    NOT NULL,
    recorded_at TIMESTAMPTZ    NOT NULL,

    CONSTRAINT orders_pk PRIMARY KEY (id),
    CONSTRAINT orders_order_id_uk UNIQUE (order_id),
    CONSTRAINT orders_status_ck CHECK (status IN ('APPROVED', 'REJECTED'))
);

-- Consulta "últimos pedidos deste cliente" — a ordenação entra no índice para
-- não virar sort em memória quando a tabela crescer.
CREATE INDEX orders_customer_id_idx ON orders (customer_id, created_at DESC);

CREATE SEQUENCE invoice_record_seq START WITH 1 INCREMENT BY 50;

CREATE TABLE invoices (
    id          BIGINT         NOT NULL,
    invoice_id  VARCHAR(36)    NOT NULL,
    order_id    VARCHAR(36)    NOT NULL,
    customer_id VARCHAR(64)    NOT NULL,
    total       NUMERIC(19, 2) NOT NULL,
    issued_at   TIMESTAMPTZ    NOT NULL,
    recorded_at TIMESTAMPTZ    NOT NULL,

    CONSTRAINT invoices_pk PRIMARY KEY (id),
    CONSTRAINT invoices_invoice_id_uk UNIQUE (invoice_id)
);

-- De propósito um índice, e não uma FOREIGN KEY para orders(order_id).
--
-- As duas tabelas são alimentadas por grupos de consumo independentes, sobre
-- tópicos diferentes. Não existe ordem garantida entre grupos: a fatura pode
-- ser gravada antes do pedido que a originou. Uma FK transformaria essa corrida
-- normal em erro de integridade, e o consumidor de faturas passaria a depender
-- do progresso do consumidor de pedidos. A integridade referencial aqui é
-- eventual, e é isso que o índice assume.
CREATE INDEX invoices_order_id_idx ON invoices (order_id);
