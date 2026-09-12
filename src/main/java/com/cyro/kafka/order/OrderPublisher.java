package com.cyro.kafka.order;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.cyro.kafka.config.Topics;

/**
 * Publica pedidos em {@code orders.v1}.
 *
 * <p>Usa o {@code KafkaTemplate} autoconfigurado pelo Boot — o
 * <b>não transacional</b>. O produtor transacional é outro bean, criado à mão em
 * {@code KafkaTransactionConfig}, e serve só ao fluxo read-process-write.
 */
@Component
public class OrderPublisher {

	private static final Logger log = LoggerFactory.getLogger(OrderPublisher.class);

	private final KafkaTemplate<String, Object> kafkaTemplate;

	public OrderPublisher(@Qualifier("kafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	/**
	 * Envia o pedido e devolve um future que completa quando o broker confirma.
	 *
	 * <p>A <b>chave é o customerId</b>, não o orderId. Kafka garante ordem apenas
	 * dentro de uma partição, e a partição é escolhida por {@code hash(key)}.
	 * Chaveando por cliente, todos os pedidos de um mesmo cliente caem na mesma
	 * partição e são processados na ordem em que chegaram. Se a chave fosse o
	 * orderId — único por mensagem — cada pedido cairia em uma partição qualquer
	 * e a ordem por cliente se perderia.
	 *
	 * <p>O {@code send} é assíncrono: ele entrega ao buffer do produtor e volta na
	 * hora. Quem quiser a confirmação do broker precisa do future (aqui, um
	 * {@code whenComplete} para log; o controller usa {@code join()} para só
	 * responder 202 depois do ack).
	 */
	public CompletableFuture<SendResult<String, Object>> publish(Order order) {
		ProducerRecord<String, Object> record =
				new ProducerRecord<>(Topics.ORDERS, order.customerId(), order);

		return this.kafkaTemplate.send(record).whenComplete((result, ex) -> {
			if (ex != null) {
				log.error("falha ao publicar pedido {}", order.orderId(), ex);
				return;
			}
			var metadata = result.getRecordMetadata();
			log.info("pedido {} publicado em {}-{} offset {}",
					order.orderId(), metadata.topic(), metadata.partition(), metadata.offset());
		});
	}

}
