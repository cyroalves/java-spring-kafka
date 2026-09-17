package com.cyro.kafka;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Sobe um Kafka e um Postgres de verdade para os testes.
 *
 * <p>{@code @ServiceConnection} é o ponto: o Boot lê host e porta de cada
 * container e injeta em {@code spring.kafka.bootstrap-servers} e em
 * {@code spring.datasource.*} sozinho. Sem isso seria preciso um
 * {@code @DynamicPropertySource} para repassar as portas aleatórias.
 *
 * <p>Serviço real em vez de mock, e Postgres real em vez de H2, pelo mesmo
 * motivo: transação, rebalance, {@code read_committed} e o comportamento da DLT
 * não existem em mock; e {@code TIMESTAMPTZ}, sequence com {@code INCREMENT BY}
 * e violação de constraint não se comportam igual em banco em memória. Um teste
 * que passa contra H2 e falha contra Postgres testou o H2.
 *
 * <p>Imagens com versão fixa, nunca {@code latest} — teste que muda de
 * comportamento sozinho quando a tag remota é atualizada não é teste. A versão do
 * Postgres é a mesma do {@code docker-compose.yml}.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	@Bean
	@ServiceConnection
	KafkaContainer kafkaContainer() {
		return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.0.0"));
	}

	@Bean
	@ServiceConnection
	PostgreSQLContainer postgresContainer() {
		return new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine"));
	}

}
