package com.cyro.kafka;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;

import com.cyro.kafka.config.Topics;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Teste ponta a ponta contra um broker Kafka real.
 *
 * <p>Cobre os três caminhos que definem o projeto: o pedido bom vira fatura, o
 * pedido permanentemente inválido vai para a DLT, e o pedido com falha temporária
 * se recupera sozinho depois do backoff.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class OrderPipelineIntegrationTest {

	@LocalServerPort
	private int port;

	@Autowired
	private KafkaProperties kafkaProperties;

	@Autowired
	private KafkaConnectionDetails connectionDetails;

	/** Cliente HTTP do próprio JDK: um teste a menos dependendo de biblioteca. */
	private final HttpClient httpClient = HttpClient.newHttpClient();

	@Test
	@DisplayName("pedido válido percorre o pipeline e vira fatura committed")
	void validOrderProducesInvoice() {
		int status = post("customer-test", "250.00");

		assertThat(status).isEqualTo(202);

		List<ConsumerRecord<String, String>> invoices = invoicesFor("customer-test", Duration.ofSeconds(30));

		assertThat(invoices).hasSize(1);
		// A chave é o customerId — é ela que mantém a ordem por cliente.
		assertThat(invoices.get(0).key()).isEqualTo("customer-test");
		// 250.00 + 10% de imposto.
		assertThat(invoices.get(0).value()).contains("\"total\":275.00");
	}

	@Test
	@DisplayName("pedido acima do limite vai direto para a DLT, sem retry")
	void invalidOrderGoesToDeadLetterTopic() {
		post("customer-invalid", "99999.00");

		List<ConsumerRecord<String, String>> deadLetters =
				deadLettersFor("customer-invalid", Duration.ofSeconds(30));

		assertThat(deadLetters).hasSize(1);
		assertThat(deadLetters.get(0).value()).contains("customer-invalid");

		// O payload original é preservado; a causa vem nos headers kafka_dlt-*.
		// É isso que permite investigar (e reprocessar) uma dead letter.
		String reason = header(deadLetters.get(0), "kafka_dlt-exception-message");
		assertThat(reason).contains("amount acima do limite");
		assertThat(header(deadLetters.get(0), "kafka_dlt-original-topic")).isEqualTo(Topics.ORDERS);
	}

	@Test
	@DisplayName("falha temporária se recupera no retry e não gera dead letter")
	void transientFailureRecoversAndProducesInvoice() {
		// O listener falha nas duas primeiras tentativas deste cliente e passa na
		// terceira — o backoff do DefaultErrorHandler cobre a diferença.
		post("flaky-customer", "500.00");

		List<ConsumerRecord<String, String>> invoices = invoicesFor("flaky-customer", Duration.ofSeconds(30));

		assertThat(invoices).hasSize(1);
		assertThat(invoices.get(0).value()).contains("\"total\":550.00");

		// Recuperou dentro do orçamento de tentativas: nada na DLT para ele.
		assertThat(deadLettersFor("flaky-customer", Duration.ofSeconds(5))).isEmpty();
	}

	private List<ConsumerRecord<String, String>> invoicesFor(String customerId, Duration timeout) {
		return TestConsumers.drain(this.kafkaProperties, this.connectionDetails, Topics.INVOICES,
				record -> customerId.equals(record.key()), 1, timeout);
	}

	private List<ConsumerRecord<String, String>> deadLettersFor(String customerId, Duration timeout) {
		return TestConsumers.drain(this.kafkaProperties, this.connectionDetails, Topics.ORDERS_DLT,
				record -> customerId.equals(record.key()), 1, timeout);
	}

	private int post(String customerId, String amount) {
		String body = """
				{"customerId": "%s", "amount": %s}""".formatted(customerId, amount);

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create("http://localhost:" + this.port + "/api/orders"))
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(body))
				.build();
		try {
			return this.httpClient.send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
		}
		catch (IOException ex) {
			throw new IllegalStateException("falha no POST de teste", ex);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrompido no POST de teste", ex);
		}
	}

	private static String header(ConsumerRecord<String, String> record, String name) {
		var header = record.headers().lastHeader(name);
		return (header != null) ? new String(header.value(), java.nio.charset.StandardCharsets.UTF_8) : null;
	}

}
