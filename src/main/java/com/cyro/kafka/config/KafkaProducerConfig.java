package com.cyro.kafka.config;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * O produtor comum — o que a API HTTP usa.
 *
 * <p>Normalmente o Boot cria {@code ProducerFactory} e {@code KafkaTemplate}
 * sozinho e não é preciso escrever nada disto. Aqui ele <b>não</b> cria: as duas
 * autoconfigurações são {@code @ConditionalOnMissingBean}, e o
 * {@link KafkaTransactionConfig} já declara um produtor transacional. Ao ver um
 * {@code ProducerFactory} qualquer no contexto, o Boot recua — inclusive do
 * produtor comum, que ninguém pediu para substituir.
 *
 * <p>Esse detalhe custa tempo quando aparece: o sintoma é
 * {@code NoSuchBeanDefinitionException} para {@code kafkaTemplate}, um bean que
 * "deveria existir". Precisando de dois produtores, declare os dois.
 */
@Configuration
public class KafkaProducerConfig {

	private final KafkaProperties kafkaProperties;

	private final KafkaConnectionDetails connectionDetails;

	public KafkaProducerConfig(KafkaProperties kafkaProperties, KafkaConnectionDetails connectionDetails) {
		this.kafkaProperties = kafkaProperties;
		this.connectionDetails = connectionDetails;
	}

	/**
	 * Propriedades de produtor com o endereço do broker vindo do lugar certo.
	 *
	 * <p>{@code KafkaProperties.buildProducerProperties()} devolve o
	 * {@code bootstrap-servers} do {@code application.yml} — e só. Quem fornece o
	 * endereço real é o {@link KafkaConnectionDetails}: em produção ele apenas
	 * repete o properties, mas sob {@code @ServiceConnection} (Testcontainers) ou
	 * Docker Compose ele aponta para a porta efêmera do container.
	 *
	 * <p>Ignorar isso é um bug silencioso e desagradável: os listeners
	 * autoconfigurados vão para o broker do teste, os produtores declarados à mão
	 * vão para o broker do {@code application.yml}, e o teste passa a ler um
	 * tópico que ninguém escreveu — ou, pior, um de outra execução.
	 */
	Map<String, Object> producerProperties() {
		Map<String, Object> configs = this.kafkaProperties.buildProducerProperties();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.connectionDetails.getBootstrapServers());
		return configs;
	}

	/**
	 * Produtor sem transação: idempotente e com {@code acks=all}, mas sem
	 * {@code transactional.id}. Suficiente para publicar pedidos vindos de um
	 * request HTTP sem pagar o custo de coordenação de uma transação.
	 */
	@Bean(autowireCandidate = false)
	ProducerFactory<String, Object> orderProducerFactory() {
		return new DefaultKafkaProducerFactory<>(producerProperties());
	}

	/**
	 * Mantém o nome {@code kafkaTemplate} de propósito: é o nome que o Boot usaria
	 * e é o que os pontos de injeção qualificam.
	 */
	@Bean
	KafkaTemplate<String, Object> kafkaTemplate() {
		return new KafkaTemplate<>(orderProducerFactory());
	}

}
