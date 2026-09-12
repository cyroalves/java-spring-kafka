package com.cyro.kafka.deadletter;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.cyro.kafka.order.Order;

/**
 * Inspeciona a dead letter queue.
 *
 * <p>A DLT não é lixo: é o inventário do que o pipeline não conseguiu processar.
 * Sem alguém olhando, a DLT vira um buraco silencioso — mensagens somem do fluxo
 * e ninguém percebe. Aqui o consumidor apenas registra; em produção ele
 * alimentaria um painel, um alerta e um botão de reprocessar.
 *
 * <p>O {@code DeadLetterPublishingRecoverer} preserva o payload original e
 * <b>anexa headers</b> com a causa: tópico/partição/offset de origem, grupo,
 * classe e mensagem da exceção, stacktrace. É isso que torna a DLT investigável.
 *
 * <p>Este listener usa ack manual (fábrica {@code manualAckListenerContainerFactory}):
 * o offset só avança depois que o registro foi tratado. É o mesmo commit manual do
 * consumidor Go do projeto irmão, só que declarativo.
 */
@Component
public class DeadLetterListener {

	private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

	@KafkaListener(
			topics = "#{T(com.cyro.kafka.config.Topics).ORDERS_DLT}",
			groupId = "dead-letter-inspector",
			containerFactory = "manualAckListenerContainerFactory")
	public void onDeadLetter(
			@Payload Order order,
			@Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) byte[] originalTopic,
			@Header(name = KafkaHeaders.DLT_ORIGINAL_PARTITION, required = false) byte[] originalPartition,
			@Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) byte[] originalOffset,
			@Header(name = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false) byte[] exceptionClass,
			@Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) byte[] exceptionMessage,
			Acknowledgment acknowledgment) {

		log.error("""
				DEAD LETTER
				  pedido    : {}
				  cliente   : {}
				  origem    : {}-{} offset {}
				  exceção   : {}
				  motivo    : {}""",
				order.orderId(),
				order.customerId(),
				asString(originalTopic),
				asInt(originalPartition),
				asLong(originalOffset),
				asString(exceptionClass),
				asString(exceptionMessage));

		// Trabalho de verdade (persistir, alertar) viria aqui. Só depois o offset
		// avança — se o processo morrer antes, a mensagem é reentregue.
		acknowledgment.acknowledge();
	}

	/**
	 * Headers do Kafka são {@code byte[]} puros — não há tipagem no protocolo.
	 * O recoverer grava strings em UTF-8 e números em big-endian.
	 */
	private static String asString(byte[] value) {
		return (value != null) ? new String(value, java.nio.charset.StandardCharsets.UTF_8) : "?";
	}

	private static String asInt(byte[] value) {
		return (value != null && value.length >= Integer.BYTES)
				? String.valueOf(ByteBuffer.wrap(value).getInt()) : "?";
	}

	private static String asLong(byte[] value) {
		return (value != null && value.length >= Long.BYTES)
				? String.valueOf(ByteBuffer.wrap(value).getLong()) : "?";
	}

}
