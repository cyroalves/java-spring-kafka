package com.cyro.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * Consumidor de apoio para os testes.
 *
 * <p>Lê o payload como {@code String} crua de propósito: o teste verifica o que
 * chegou <b>no tópico</b>, não o que a aplicação conseguiria desserializar. Se o
 * assert dependesse do mesmo deserializer da aplicação, um bug no contrato
 * passaria despercebido.
 */
final class TestConsumers {

	private TestConsumers() {
	}

	/**
	 * Consome o tópico desde o início e devolve só os registros que casam com
	 * {@code predicate}, parando assim que juntar {@code expected} deles.
	 *
	 * <p>Filtrar em vez de contar tudo é o que torna os testes independentes: eles
	 * compartilham o mesmo broker e o mesmo tópico, então um teste sempre enxerga
	 * o que os outros escreveram. Cada teste usa um {@code customerId} próprio e
	 * só olha para o que é seu.
	 *
	 * <p>Grupo novo a cada chamada ({@code UUID}) + {@code auto.offset.reset=earliest}
	 * para ler desde o começo, sem depender de offsets de execuções anteriores.
	 */
	static List<ConsumerRecord<String, String>> drain(
			KafkaProperties properties, KafkaConnectionDetails connectionDetails,
			String topic, Predicate<ConsumerRecord<String, String>> predicate,
			int expected, Duration timeout) {

		Map<String, Object> configs = properties.buildConsumerProperties();
		// O endereço vem do KafkaConnectionDetails, não do application.yml: sob
		// Testcontainers é a porta efêmera do container que vale.
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getBootstrapServers());
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configs.remove("spring.deserializer.value.delegate.class");

		List<ConsumerRecord<String, String>> collected = new ArrayList<>();
		try (Consumer<String, String> consumer =
				new DefaultKafkaConsumerFactory<String, String>(configs).createConsumer()) {

			consumer.subscribe(List.of(topic));
			long deadline = System.currentTimeMillis() + timeout.toMillis();
			while (System.currentTimeMillis() < deadline && collected.size() < expected) {
				ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
				batch.forEach(record -> {
					if (predicate.test(record)) {
						collected.add(record);
					}
				});
			}
		}
		return collected;
	}

}
