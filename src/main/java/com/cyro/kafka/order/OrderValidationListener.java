package com.cyro.kafka.order;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Valida pedidos — o consumidor que demonstra retry, backoff e DLT.
 *
 * <p>Lê de {@code orders.v1} no grupo {@code order-validation}. O
 * {@code InvoiceProcessor} lê do <b>mesmo tópico</b> em outro grupo: essa é a
 * diferença entre Kafka e uma fila tradicional. Cada grupo tem seu próprio
 * ponteiro de leitura (offset) e recebe <em>todas</em> as mensagens; dentro de um
 * grupo, cada partição vai para um consumidor só.
 *
 * <p>As "regras de negócio" abaixo são gatilhos determinísticos para demonstrar
 * cada caminho de erro sem precisar derrubar nada.
 */
@Component
public class OrderValidationListener {

	private static final Logger log = LoggerFactory.getLogger(OrderValidationListener.class);

	/**
	 * Quantas vezes cada pedido já foi tentado.
	 *
	 * <p>Em memória e por instância — serve para a demo de retry, não é estado de
	 * produção: morre no restart e não é compartilhado entre instâncias.
	 * Idempotência de verdade é uma tabela de ids já processados, consultada antes
	 * de aplicar o efeito. É ela que torna at-least-once seguro.
	 */
	private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

	@KafkaListener(
			topics = "#{T(com.cyro.kafka.config.Topics).ORDERS}",
			groupId = "order-validation")
	public void onOrder(
			@Payload Order order,
			@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
			@Header(KafkaHeaders.OFFSET) long offset) {

		int attempt = this.attempts
				.computeIfAbsent(order.orderId(), key -> new AtomicInteger())
				.incrementAndGet();

		log.info("validando {} (cliente {}) partição {} offset {} tentativa {}",
				order.orderId(), order.customerId(), partition, offset, attempt);

		// --- falha permanente: o dado está errado e vai continuar errado ---
		OrderRules.validate(order);

		// --- falha temporária que nunca se resolve: esgota o backoff e vai para a DLT ---
		if (order.customerId().contains("boom")) {
			throw new TransientFailureException(
					"serviço de crédito indisponível (tentativa " + attempt + ")");
		}

		// --- falha temporária que se resolve: as duas primeiras falham, a terceira passa ---
		if (order.customerId().contains("flaky") && attempt <= 2) {
			throw new TransientFailureException(
					"timeout no serviço de crédito (tentativa " + attempt + ")");
		}

		log.info("pedido {} aprovado", order.orderId());
	}

}
