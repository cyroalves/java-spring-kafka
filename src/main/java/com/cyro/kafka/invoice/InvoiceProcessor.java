package com.cyro.kafka.invoice;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.cyro.kafka.config.Topics;
import com.cyro.kafka.order.Order;
import com.cyro.kafka.order.OrderRules;

/**
 * Read-process-write transacional: lê pedido, emite fatura, tudo ou nada.
 *
 * <p>Este é o ponto mais sutil do projeto. O container deste listener roda com um
 * {@code KafkaTransactionManager} (ver {@code KafkaTransactionConfig}), então:
 *
 * <ol>
 *   <li>antes de chamar o método, o container abre uma transação no produtor;</li>
 *   <li>o {@code send} da fatura entra nessa transação;</li>
 *   <li>ao retornar sem exceção, o container manda o <b>offset do pedido</b> para
 *       dentro da mesma transação ({@code sendOffsetsToTransaction}) e commita.</li>
 * </ol>
 *
 * <p>O ganho: a fatura e o avanço do offset commitam juntos. Sem isso existem dois
 * buracos clássicos — publicar a fatura e morrer antes de commitar o offset (fatura
 * duplicada no reprocessamento), ou commitar o offset e morrer antes de publicar
 * (pedido some sem fatura).
 *
 * <p>Isso é "exactly-once" <b>dentro do Kafka</b>: cobre escritas em tópicos mais
 * o commit do offset, e nada além. Não alcança banco, cache ou chamada HTTP — para
 * esses o caminho é outbox transacional mais idempotência no consumidor.
 */
@Component
public class InvoiceProcessor {

	private static final Logger log = LoggerFactory.getLogger(InvoiceProcessor.class);

	private static final BigDecimal TAX_RATE = new BigDecimal("0.10");

	/** Pedidos neste valor exato falham de propósito, para demonstrar rollback. */
	private static final BigDecimal ROLLBACK_TRIGGER = new BigDecimal("6666.00");

	private final KafkaTemplate<String, Object> transactionalKafkaTemplate;

	public InvoiceProcessor(
			@Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, Object> transactionalKafkaTemplate) {
		this.transactionalKafkaTemplate = transactionalKafkaTemplate;
	}

	@KafkaListener(
			topics = "#{T(com.cyro.kafka.config.Topics).ORDERS}",
			groupId = "invoicing",
			containerFactory = "transactionalListenerContainerFactory")
	public void onOrder(Order order) {
		log.info("faturando pedido {}", order.orderId());

		if (!OrderRules.isAcceptable(order)) {
			// Pedido inválido não gera fatura. Sem exceção de propósito: a
			// transação commita apenas o avanço do offset. Quem registra o
			// problema é a validação, que manda o pedido para a DLT — duplicar o
			// alarme nos dois grupos só geraria ruído.
			log.warn("pedido {} ignorado no faturamento: reprovado nas regras", order.orderId());
			return;
		}

		BigDecimal total = order.amount()
				.multiply(BigDecimal.ONE.add(TAX_RATE))
				.setScale(2, RoundingMode.HALF_UP);

		Invoice invoice = new Invoice(
				UUID.randomUUID().toString(),
				order.orderId(),
				order.customerId(),
				total,
				Instant.now());

		// Este send já está dentro da transação aberta pelo container.
		this.transactionalKafkaTemplate.send(Topics.INVOICES, order.customerId(), invoice);
		log.info("fatura {} enfileirada na transação (pedido {})",
				invoice.invoiceId(), order.orderId());

		if (order.amount().compareTo(ROLLBACK_TRIGGER) == 0) {
			// Exceção depois do send: a transação aborta. A fatura chegou a ser
			// escrita no log, mas fica marcada como abortada e consumidores em
			// read_committed nunca a enxergam.
			throw new IllegalStateException(
					"falha proposital após o send — a fatura acima será descartada no rollback");
		}
	}

}
