package com.cyro.kafka.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;

import com.cyro.kafka.order.InvalidOrderException;

/**
 * O que acontece quando um listener lança exceção.
 *
 * <p>Sem nada configurado, o Spring já repete 10 vezes seguidas, sem pausa, e
 * depois loga e segue em frente — a mensagem some. Dois problemas: sem pausa, uma
 * indisponibilidade de 2 segundos vira 10 falhas em milissegundos; e "logar e
 * seguir" perde o dado. Este arquivo troca isso por backoff exponencial e dead
 * letter queue.
 */
@Configuration
public class KafkaErrorHandlingConfig {

	/**
	 * Error handler aplicado à fábrica padrão do Boot.
	 *
	 * <p>O Boot procura um bean {@code CommonErrorHandler} no contexto e o injeta
	 * na {@code kafkaListenerContainerFactory} autoconfigurada — por isso basta
	 * declarar o bean, sem tocar na fábrica.
	 *
	 * <p>Recebe o template <b>não transacional</b> (o autoconfigurado): as
	 * mensagens mortas deste fluxo não fazem parte de nenhuma transação. O fluxo
	 * transacional tem o seu próprio error handler, ver {@link KafkaTransactionConfig}.
	 */
	@Bean
	DefaultErrorHandler defaultErrorHandler(
			@Qualifier("kafkaTemplate") KafkaOperations<String, Object> kafkaTemplate) {

		DefaultErrorHandler handler = new DefaultErrorHandler(deadLetterRecoverer(kafkaTemplate), backOff());
		classifyExceptions(handler);
		return handler;
	}

	/**
	 * Backoff exponencial: 500ms, 1s, 2s, 4s — depois desiste e chama o recoverer.
	 *
	 * <p>Durante a espera <b>a partição inteira fica parada</b>. O container pausa
	 * o consumidor e faz seek de volta ao offset que falhou; ele não pode pular
	 * adiante sem quebrar a ordem nem perder a mensagem. Por isso o
	 * {@code maxInterval} é baixo: backoff generoso demais em consumidor Kafka é
	 * indisponibilidade disfarçada de resiliência.
	 */
	static ExponentialBackOff backOff() {
		ExponentialBackOff backOff = new ExponentialBackOff();
		backOff.setInitialInterval(500L);
		backOff.setMultiplier(2.0);
		backOff.setMaxInterval(4_000L);
		backOff.setMaxAttempts(4);
		return backOff;
	}

	/**
	 * Recoverer que publica o registro morto em {@code <tópico>.DLT}.
	 *
	 * <p>O destino é resolvido por uma função explícita em vez do padrão do
	 * framework. Dois motivos: o sufixo padrão mudou entre as linhas 3.x e 4.x do
	 * spring-kafka, e um destino implícito só se revela errado em produção, na
	 * forma de um produtor travado em {@code UNKNOWN_TOPIC_OR_PARTITION}.
	 *
	 * <p>A partição é preservada: o registro morto vai para a mesma partição de
	 * origem, então a DLT precisa ter pelo menos o mesmo número de partições.
	 *
	 * <p>Ele mantém chave e payload originais e acrescenta headers
	 * {@code kafka_dlt-*} com origem, exceção e stacktrace — é o que torna a DLT
	 * investigável em vez de um amontoado de JSON sem contexto.
	 */
	static DeadLetterPublishingRecoverer deadLetterRecoverer(KafkaOperations<?, ?> template) {
		return new DeadLetterPublishingRecoverer(template,
				(record, exception) -> new TopicPartition(
						record.topic() + Topics.DLT_SUFFIX, record.partition()));
	}

	/**
	 * Separa o que vale repetir do que não vale.
	 *
	 * <p>{@link InvalidOrderException} é defeito do dado: repetir não muda nada,
	 * então pula o backoff e vai direto para a DLT. {@link DeserializationException}
	 * idem — bytes que não viram objeto nunca virão a virar. Todo o resto (timeout,
	 * indisponibilidade) é tratado como temporário e passa pelo backoff.
	 */
	static void classifyExceptions(DefaultErrorHandler handler) {
		handler.addNotRetryableExceptions(InvalidOrderException.class, DeserializationException.class);
	}

	/**
	 * Fábrica com <b>ack manual</b>, usada pelo inspetor da DLT.
	 *
	 * <p>{@code MANUAL_IMMEDIATE} entrega um {@link org.springframework.kafka.support.Acknowledgment}
	 * ao método e só commita o offset quando o código chama {@code acknowledge()}.
	 * A diferença para o modo {@code RECORD} (usado no resto do projeto) é quem
	 * decide o momento do commit: aqui o código, lá o container.
	 *
	 * <p>Nenhum dos dois é "melhor": commitar antes de processar dá at-most-once
	 * (perde em crash), commitar depois dá at-least-once (duplica em crash). Kafka
	 * não oferece terceira opção para efeitos fora do broker — por isso consumidor
	 * precisa ser idempotente.
	 */
	@Bean
	ConcurrentKafkaListenerContainerFactory<Object, Object> manualAckListenerContainerFactory(
			ConsumerFactory<Object, Object> consumerFactory) {

		ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setConcurrency(1);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		return factory;
	}

}
