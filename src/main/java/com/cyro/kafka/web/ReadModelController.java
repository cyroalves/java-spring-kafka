package com.cyro.kafka.web;

import java.util.List;

import org.springframework.data.domain.Limit;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.cyro.kafka.store.InvoiceRecordRepository;
import com.cyro.kafka.store.OrderRecordRepository;

/**
 * Consulta ao read model — o lado de leitura do pipeline.
 *
 * <p>Separado do {@link OrderController} de propósito: lá se escreve (publica no
 * Kafka e responde 202, sem saber o resultado), aqui se lê (consulta o Postgres,
 * responde 200 com o que já foi processado). São dois caminhos com latências,
 * modos de falha e contratos diferentes.
 *
 * <p>Consequência que vale entender: um {@code GET} logo depois do {@code POST}
 * pode devolver 404. O pedido foi aceito e está durável no log do Kafka, mas o
 * consumidor pode ainda não o ter gravado. Isso é <b>consistência eventual</b>, e
 * é o preço de ler de uma projeção em vez do sistema que recebeu a escrita.
 */
@RestController
public class ReadModelController {

	/** Teto de itens por consulta: sem ele, um {@code limit=1000000} vira um scan. */
	private static final int MAX_LIMIT = 200;

	private final OrderRecordRepository orders;

	private final InvoiceRecordRepository invoices;

	public ReadModelController(OrderRecordRepository orders, InvoiceRecordRepository invoices) {
		this.orders = orders;
		this.invoices = invoices;
	}

	/**
	 * Pedidos gravados, do mais recente para o mais antigo.
	 *
	 * <p>Devolve {@code List<OrderView>} e não {@code Page<OrderRecord>}: o
	 * {@code Page} tem serialização JSON instável entre versões do Spring Data (o
	 * próprio Boot alerta sobre isso), e a entidade não deve vazar para o
	 * contrato HTTP.
	 */
	@GetMapping("/api/orders")
	public List<OrderView> listOrders(
			@RequestParam(required = false) String customerId,
			@RequestParam(defaultValue = "20") int limit) {

		Limit cap = Limit.of(Math.clamp(limit, 1, MAX_LIMIT));

		return ((customerId != null)
				? this.orders.findByCustomerIdOrderByCreatedAtDesc(customerId, cap)
				: this.orders.findAllByOrderByCreatedAtDesc(cap))
				.stream()
				.map(OrderView::from)
				.toList();
	}

	/** Um pedido e as faturas que ele gerou. 404 enquanto o consumidor não gravou. */
	@GetMapping("/api/orders/{orderId}")
	public ResponseEntity<OrderDetailView> getOrder(@PathVariable String orderId) {
		return this.orders.findByOrderId(orderId)
				.map(record -> new OrderDetailView(
						OrderView.from(record),
						this.invoices.findByOrderId(orderId).stream().map(InvoiceView::from).toList()))
				.map(ResponseEntity::ok)
				.orElseGet(() -> ResponseEntity.notFound().build());
	}

	@GetMapping("/api/invoices")
	public List<InvoiceView> listInvoices(
			@RequestParam(required = false) String customerId,
			@RequestParam(defaultValue = "20") int limit) {

		Limit cap = Limit.of(Math.clamp(limit, 1, MAX_LIMIT));

		return ((customerId != null)
				? this.invoices.findByCustomerIdOrderByIssuedAtDesc(customerId, cap)
				: this.invoices.findAllByOrderByIssuedAtDesc(cap))
				.stream()
				.map(InvoiceView::from)
				.toList();
	}

	/**
	 * Resumo por cliente.
	 *
	 * <p>{@code @Transactional(readOnly = true)} não está aqui por hábito: são
	 * três consultas, e sem uma transação em volta cada uma abriria a sua e veria
	 * um estado diferente do banco — o total podia incluir um pedido que a lista
	 * abaixo dele não mostra. Com a transação, as três leem o mesmo snapshot.
	 *
	 * <p>{@code readOnly} também informa o Hibernate de que não vale a pena manter
	 * snapshot das entidades para dirty checking no flush.
	 */
	@GetMapping("/api/customers/{customerId}")
	@Transactional(readOnly = true)
	public CustomerSummaryView getCustomer(
			@PathVariable String customerId,
			@RequestParam(defaultValue = "20") int limit) {

		Limit cap = Limit.of(Math.clamp(limit, 1, MAX_LIMIT));

		return new CustomerSummaryView(
				customerId,
				this.orders.sumApprovedAmount(customerId),
				this.orders.findByCustomerIdOrderByCreatedAtDesc(customerId, cap)
						.stream().map(OrderView::from).toList(),
				this.invoices.findByCustomerIdOrderByIssuedAtDesc(customerId, cap)
						.stream().map(InvoiceView::from).toList());
	}

}
