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
import org.springframework.data.domain.Limit;

import com.cyro.kafka.config.Topics;
import com.cyro.kafka.store.InvoiceRecord;
import com.cyro.kafka.store.InvoiceRecordRepository;
import com.cyro.kafka.store.OrderRecord;
import com.cyro.kafka.store.OrderRecordRepository;
import com.cyro.kafka.store.OrderStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Teste ponta a ponta contra um broker Kafka real.
 *
 * <p>Cobre os caminhos que definem o projeto: o pedido bom vira fatura, o pedido
 * permanentemente inválido vai para a DLT, o pedido com falha temporária se
 * recupera sozinho depois do backoff — e, do lado do read model, o que cada um
 * desses caminhos deixa (ou não deixa) gravado no Postgres.
 *
 * <p>Os asserts de banco esperam com Awaitility em vez de olhar logo depois do
 * {@code POST}. A resposta 202 significa "o Kafka aceitou", não "o consumidor já
 * gravou"; assert imediato testaria a velocidade da máquina.
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

	@Autowired
	private OrderRecordRepository orderRecords;

	@Autowired
	private InvoiceRecordRepository invoiceRecords;

	/** Cliente HTTP do próprio JDK: um teste a menos dependendo de biblioteca. */
	private final HttpClient httpClient = HttpClient.newHttpClient();

	@Test
	@DisplayName("pedido válido percorre o pipeline e vira fatura committed")
	void validOrderProducesInvoice() {
		int status = post("customer-test", "250.00").statusCode();

		assertThat(status).isEqualTo(202);

		List<ConsumerRecord<String, String>> invoices = invoicesFor("customer-test", Duration.ofSeconds(30));

		assertThat(invoices).hasSize(1);
		// A chave é o customerId — é ela que mantém a ordem por cliente.
		assertThat(invoices.get(0).key()).isEqualTo("customer-test");
		// 250.00 + 10% de imposto.
		assertThat(invoices.get(0).value()).contains("\"total\":275.00");

		// O mesmo evento, agora pela projeção: dois consumidores independentes
		// gravaram cada um a sua tabela.
		await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
			assertThat(ordersOf("customer-test")).hasSize(1);
			assertThat(invoicesOf("customer-test")).hasSize(1);
		});

		assertThat(ordersOf("customer-test").getFirst().getStatus()).isEqualTo(OrderStatus.APPROVED);
		assertThat(invoicesOf("customer-test").getFirst().getTotal()).isEqualByComparingTo("275.00");
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

		// Ir para a DLT não apaga o pedido do read model: o grupo order-store tem
		// offset próprio e grava o que viu, com o veredito das regras.
		await().atMost(Duration.ofSeconds(30))
				.untilAsserted(() -> assertThat(ordersOf("customer-invalid")).hasSize(1));

		assertThat(ordersOf("customer-invalid").getFirst().getStatus()).isEqualTo(OrderStatus.REJECTED);
		// Reprovado nas regras não gera fatura — nem no tópico, nem na tabela.
		assertThat(invoicesOf("customer-invalid")).isEmpty();
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

		// As três tentativas do grupo order-validation não viram três linhas: o
		// grupo order-store é outro, e a constraint UNIQUE(order_id) cobre a
		// reentrega dentro dele.
		await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
			assertThat(ordersOf("flaky-customer")).hasSize(1);
			assertThat(invoicesOf("flaky-customer")).hasSize(1);
		});
	}

	@Test
	@DisplayName("rollback descarta as faturas abortadas, menos a da tentativa recuperada")
	void rollbackDiscardsAbortedInvoicesExceptTheRecoveredOne() {
		// 6666.00 faz o InvoiceProcessor publicar a fatura e então falhar. Cada
		// tentativa escreve uma fatura nova no log e aborta a transação.
		post("customer-rollback", "6666.00");

		// Esperar a dead letter garante que o processador transacional já esgotou
		// o orçamento de tentativas.
		assertThat(deadLettersFor("customer-rollback", Duration.ofSeconds(60))).hasSize(1);

		await().atMost(Duration.ofSeconds(30))
				.untilAsserted(() -> assertThat(ordersOf("customer-rollback")).hasSize(1));

		// O pedido é válido para as regras, então o read model o gravou aprovado —
		// reprovar no faturamento não é reprovar nas regras.
		assertThat(ordersOf("customer-rollback").getFirst().getStatus()).isEqualTo(OrderStatus.APPROVED);

		// Aqui está o ponto do teste, e ele não é "nenhuma fatura".
		//
		// São cinco tentativas, cinco faturas escritas fisicamente no log. Quatro
		// ficam invisíveis: o consumidor roda em read_committed e nunca as entrega.
		// A quinta chega ao banco — quando as tentativas se esgotam e o registro
		// vai para a DLT, commitRecovered=true commita o envio da dead letter e o
		// offset em uma transação que leva junto o que o listener produziu naquela
		// última passagem.
		//
		// É a ressalva do README em forma de assert: efeito colateral que não pode
		// sobreviver a uma falha tem de ser produzido depois da parte que falha,
		// nunca antes. O read model é o que torna esse vazamento visível.
		await().atMost(Duration.ofSeconds(30))
				.untilAsserted(() -> assertThat(invoicesOf("customer-rollback")).hasSize(1));

		// 6666.00 + 10% de imposto.
		assertThat(invoicesOf("customer-rollback").getFirst().getTotal()).isEqualByComparingTo("7332.60");
	}

	@Test
	@DisplayName("a API de consulta devolve o pedido gravado com suas faturas")
	void readModelAnswersQueriesOverHttp() {
		post("customer-query", "100.00");

		await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
			assertThat(ordersOf("customer-query")).hasSize(1);
			assertThat(invoicesOf("customer-query")).hasSize(1);
		});

		String orderId = ordersOf("customer-query").getFirst().getOrderId();

		HttpResponse<String> detail = get("/api/orders/" + orderId);

		assertThat(detail.statusCode()).isEqualTo(200);
		assertThat(detail.body())
				.contains(orderId)
				.contains("\"status\":\"APPROVED\"")
				.contains("\"total\":110.00");

		// Consistência eventual tem cara de 404: id que o consumidor ainda não
		// gravou (ou que nunca existiu) não está na projeção.
		assertThat(get("/api/orders/id-que-nao-existe").statusCode()).isEqualTo(404);

		// A pergunta que justifica a tabela existir: agregação por cliente.
		assertThat(get("/api/customers/customer-query").body())
				.contains("\"approvedTotal\":100.00");
	}

	private List<ConsumerRecord<String, String>> invoicesFor(String customerId, Duration timeout) {
		return TestConsumers.drain(this.kafkaProperties, this.connectionDetails, Topics.INVOICES,
				record -> customerId.equals(record.key()), 1, timeout);
	}

	private List<ConsumerRecord<String, String>> deadLettersFor(String customerId, Duration timeout) {
		return TestConsumers.drain(this.kafkaProperties, this.connectionDetails, Topics.ORDERS_DLT,
				record -> customerId.equals(record.key()), 1, timeout);
	}

	/**
	 * Pedidos gravados de um cliente.
	 *
	 * <p>Filtrar por cliente não é detalhe: os testes compartilham o mesmo
	 * contexto, o mesmo container e agora também o mesmo banco. Um
	 * {@code repository.count()} aqui quebraria assim que outro teste rodasse
	 * antes — cada teste enxerga o que os outros escreveram.
	 */
	private List<OrderRecord> ordersOf(String customerId) {
		return this.orderRecords.findByCustomerIdOrderByCreatedAtDesc(customerId, Limit.of(10));
	}

	private List<InvoiceRecord> invoicesOf(String customerId) {
		return this.invoiceRecords.findByCustomerIdOrderByIssuedAtDesc(customerId, Limit.of(10));
	}

	private HttpResponse<String> post(String customerId, String amount) {
		String body = """
				{"customerId": "%s", "amount": %s}""".formatted(customerId, amount);

		return send(HttpRequest.newBuilder()
				.uri(uri("/api/orders"))
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(body))
				.build());
	}

	private HttpResponse<String> get(String path) {
		return send(HttpRequest.newBuilder().uri(uri(path)).GET().build());
	}

	private URI uri(String path) {
		return URI.create("http://localhost:" + this.port + path);
	}

	private HttpResponse<String> send(HttpRequest request) {
		try {
			return this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());
		}
		catch (IOException ex) {
			throw new IllegalStateException("falha na chamada HTTP de teste", ex);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrompido na chamada HTTP de teste", ex);
		}
	}

	private static String header(ConsumerRecord<String, String> record, String name) {
		var header = record.headers().lastHeader(name);
		return (header != null) ? new String(header.value(), java.nio.charset.StandardCharsets.UTF_8) : null;
	}

}
