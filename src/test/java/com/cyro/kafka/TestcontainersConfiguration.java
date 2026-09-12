package com.cyro.kafka;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Sobe um Kafka de verdade para os testes.
 *
 * <p>{@code @ServiceConnection} é o ponto: o Boot lê host e porta do container e
 * injeta em {@code spring.kafka.bootstrap-servers} sozinho. Sem isso seria
 * preciso um {@code @DynamicPropertySource} para repassar a porta aleatória.
 *
 * <p>Broker real em vez de mock por um motivo prático: transação, rebalance,
 * {@code read_committed} e o comportamento da DLT não existem em mock. Um teste
 * que passa contra um mock de Kafka prova pouco sobre o que roda em produção.
 *
 * <p>Imagem com versão fixa, nunca {@code latest} — teste que muda de
 * comportamento sozinho quando a tag remota é atualizada não é teste.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	@Bean
	@ServiceConnection
	KafkaContainer kafkaContainer() {
		return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.0.0"));
	}

}
