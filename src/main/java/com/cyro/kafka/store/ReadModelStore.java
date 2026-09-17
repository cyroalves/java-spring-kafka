package com.cyro.kafka.store;

import java.time.Instant;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cyro.kafka.invoice.Invoice;
import com.cyro.kafka.order.Order;
import com.cyro.kafka.order.OrderRules;

/**
 * Escreve o read model a partir dos eventos.
 *
 * <p>Por que os listeners não chamam o repositório direto: é aqui que fica o
 * limite da transação. Um {@code @Transactional} em método de
 * {@code @KafkaListener} funciona, mas esconde o ponto mais importante desta
 * classe — <b>a transação do banco e o commit do offset são duas transações
 * distintas</b>, e nada as junta:
 *
 * <pre>
 *   [ commit no Postgres ] ... crash aqui ... [ commit do offset no Kafka ]
 * </pre>
 *
 * <p>Uma falha entre as duas reentrega a mensagem e a gravação se repete. Não há
 * configuração que resolva isso: o Kafka não participa de two-phase commit com o
 * banco. O que existe é o caminho oposto — deixar duplicar e tornar a gravação
 * idempotente, que é o papel das constraints {@code UNIQUE} do V1.
 *
 * <p>O problema inverso — gravar no banco <em>e</em> publicar no Kafka
 * atomicamente — é o que o padrão outbox resolve, e este projeto não o tem:
 * nenhum listener faz as duas coisas. Estes dois métodos só gravam; o
 * {@code InvoiceProcessor} só publica.
 */
@Service
public class ReadModelStore {

	private final OrderRecordRepository orders;

	private final InvoiceRecordRepository invoices;

	public ReadModelStore(OrderRecordRepository orders, InvoiceRecordRepository invoices) {
		this.orders = orders;
		this.invoices = invoices;
	}

	/**
	 * Grava o pedido, se ainda não estiver gravado.
	 *
	 * @return {@code true} se inseriu, {@code false} se já existia
	 * @throws org.springframework.dao.DataIntegrityViolationException quando outra
	 * instância inseriu o mesmo {@code orderId} entre a checagem e o insert — a
	 * corrida que o {@code existsByOrderId} sozinho não cobre. Pode ser lançada
	 * tanto no {@code save} quanto no commit, porque o Hibernate adia o
	 * {@code INSERT} até o flush.
	 */
	@Transactional
	public boolean save(Order order) {
		if (this.orders.existsByOrderId(order.orderId())) {
			return false;
		}

		OrderStatus status = OrderRules.isAcceptable(order) ? OrderStatus.APPROVED : OrderStatus.REJECTED;

		this.orders.save(new OrderRecord(
				order.orderId(),
				order.customerId(),
				order.amount(),
				status,
				order.createdAt(),
				Instant.now()));
		return true;
	}

	/**
	 * Grava a fatura, se ainda não estiver gravada.
	 *
	 * @return {@code true} se inseriu, {@code false} se já existia
	 */
	@Transactional
	public boolean save(Invoice invoice) {
		if (this.invoices.existsByInvoiceId(invoice.invoiceId())) {
			return false;
		}

		this.invoices.save(new InvoiceRecord(
				invoice.invoiceId(),
				invoice.orderId(),
				invoice.customerId(),
				invoice.total(),
				invoice.issuedAt(),
				Instant.now()));
		return true;
	}

}
