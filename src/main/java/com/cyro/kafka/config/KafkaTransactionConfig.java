package com.cyro.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;

/**
 * Produtor transacional e container read-process-write.
 *
 * <p>Por que um produtor separado em vez de ligar
 * {@code spring.kafka.producer.transaction-id-prefix} e usar um só: assim que um
 * produtor vira transacional, <b>todo</b> envio por ele precisa estar dentro de
 * uma transação. O {@code OrderPublisher}, chamado de dentro de um request HTTP,
 * não tem motivo para pagar o custo de uma transação — ele precisa de
 * idempotência e {@code acks=all}, nada além. Dois produtores, duas
 * responsabilidades.
 */
@Configuration
public class KafkaTransactionConfig {

	/**
	 * O {@code transactional.id} é a identidade da transação no broker.
	 *
	 * <p>É ele que dá o "exactly-once": ao reiniciar, o produtor se anuncia com o
	 * mesmo id, o broker incrementa a epoch e <b>elimina</b> (fencing) qualquer
	 * instância antiga que ainda esteja escrevendo. Sem isso, um processo zumbi
	 * poderia commitar depois que o substituto já assumiu.
	 *
	 * <p>O Spring acrescenta um sufixo por produtor, então o prefixo precisa ser
	 * estável por <em>deployment</em> — não pode ser aleatório a cada boot, ou o
	 * fencing nunca acontece.
	 */
	private static final String TRANSACTIONAL_ID_PREFIX = "invoicing-tx-";

	private final KafkaProducerConfig producerConfig;

	public KafkaTransactionConfig(KafkaProducerConfig producerConfig) {
		this.producerConfig = producerConfig;
	}

	/**
	 * Produtor transacional, montado a partir das mesmas propriedades do produtor
	 * padrão mais o {@code transactional.id}.
	 *
	 * <p>{@code autowireCandidate = false} é essencial: sem isso passariam a
	 * existir dois beans {@code ProducerFactory} no contexto e a autoconfiguração
	 * do Boot — que injeta um {@code ProducerFactory<Object, Object>} para montar
	 * o {@code KafkaTemplate} padrão — ficaria ambígua. Marcado assim, este bean
	 * só é alcançável pelos métodos deste arquivo.
	 */
	@Bean(autowireCandidate = false)
	ProducerFactory<String, Object> transactionalProducerFactory() {
		DefaultKafkaProducerFactory<String, Object> factory =
				new DefaultKafkaProducerFactory<>(this.producerConfig.producerProperties());
		factory.setTransactionIdPrefix(TRANSACTIONAL_ID_PREFIX);
		return factory;
	}

	/** Template usado pelo {@code InvoiceProcessor} para emitir faturas. */
	@Bean
	KafkaTemplate<String, Object> transactionalKafkaTemplate() {
		return new KafkaTemplate<>(transactionalProducerFactory());
	}

	/**
	 * O gerente de transação que o container usa para abrir e commitar.
	 *
	 * <p>Não é o {@code @Transactional} de banco: é um
	 * {@link org.springframework.kafka.transaction.KafkaAwareTransactionManager},
	 * que sabe mandar os offsets consumidos para dentro da transação do produtor.
	 */
	@Bean(autowireCandidate = false)
	KafkaTransactionManager<String, Object> invoiceTransactionManager() {
		return new KafkaTransactionManager<>(transactionalProducerFactory());
	}

	/**
	 * Container do fluxo transacional.
	 *
	 * <p>Com o transaction manager ligado, o ciclo por registro vira:
	 * abre transação → chama o listener → {@code sendOffsetsToTransaction} →
	 * commit. Uma exceção do listener aborta tudo: a fatura é descartada e o
	 * offset não avança, então o pedido é reprocessado.
	 *
	 * <p>Concorrência 1 de propósito: cada thread de um container transacional
	 * mantém seu próprio produtor com {@code transactional.id} distinto. Funciona
	 * com mais threads, mas o número de ids passa a variar com a configuração, e
	 * a demo fica menos previsível.
	 *
	 * <p>{@code setCommitRecovered(true)} fecha o último buraco: quando as
	 * tentativas se esgotam e o registro vai para a DLT, o envio da dead letter e
	 * o avanço do offset precisam commitar <em>juntos</em>. Sem isso o processo
	 * poderia publicar a dead letter e morrer antes de commitar o offset — e o
	 * mesmo registro venenoso reapareceria na DLT a cada reinício.
	 */
	@Bean
	ConcurrentKafkaListenerContainerFactory<Object, Object> transactionalListenerContainerFactory(
			ConsumerFactory<Object, Object> consumerFactory) {

		ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setConcurrency(1);

		ContainerProperties containerProperties = factory.getContainerProperties();
		containerProperties.setKafkaAwareTransactionManager(invoiceTransactionManager());

		DefaultErrorHandler errorHandler = new DefaultErrorHandler(
				KafkaErrorHandlingConfig.deadLetterRecoverer(transactionalKafkaTemplate()),
				KafkaErrorHandlingConfig.backOff());
		errorHandler.setCommitRecovered(true);
		KafkaErrorHandlingConfig.classifyExceptions(errorHandler);
		factory.setCommonErrorHandler(errorHandler);

		return factory;
	}

}
