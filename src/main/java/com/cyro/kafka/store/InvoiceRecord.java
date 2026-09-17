package com.cyro.kafka.store;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Linha da tabela {@code invoices} — a projeção de um evento de {@code invoices.v1}.
 *
 * <p>Note o que <b>não</b> existe aqui: um {@code @ManyToOne OrderRecord}. A
 * ligação com o pedido é o {@code orderId} solto, sem chave estrangeira, porque
 * as duas tabelas são alimentadas por grupos de consumo independentes e não há
 * ordem garantida entre eles — a fatura pode chegar antes do pedido. Um
 * relacionamento JPA aqui exigiria a linha do pedido já gravada, e transformaria
 * uma corrida normal em erro.
 */
@Entity
@Table(name = "invoices")
public class InvoiceRecord {

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "invoice_record_seq")
	@SequenceGenerator(name = "invoice_record_seq", sequenceName = "invoice_record_seq", allocationSize = 50)
	private Long id;

	/** Chave natural. O {@code UNIQUE} é o que torna a gravação idempotente. */
	@Column(name = "invoice_id", nullable = false, unique = true, length = 36)
	private String invoiceId;

	@Column(name = "order_id", nullable = false, length = 36)
	private String orderId;

	@Column(name = "customer_id", nullable = false, length = 64)
	private String customerId;

	@Column(name = "total", nullable = false, precision = 19, scale = 2)
	private BigDecimal total;

	@Column(name = "issued_at", nullable = false)
	private Instant issuedAt;

	@Column(name = "recorded_at", nullable = false)
	private Instant recordedAt;

	protected InvoiceRecord() {
	}

	public InvoiceRecord(String invoiceId, String orderId, String customerId,
			BigDecimal total, Instant issuedAt, Instant recordedAt) {
		this.invoiceId = invoiceId;
		this.orderId = orderId;
		this.customerId = customerId;
		this.total = total;
		this.issuedAt = issuedAt;
		this.recordedAt = recordedAt;
	}

	public Long getId() {
		return this.id;
	}

	public String getInvoiceId() {
		return this.invoiceId;
	}

	public String getOrderId() {
		return this.orderId;
	}

	public String getCustomerId() {
		return this.customerId;
	}

	public BigDecimal getTotal() {
		return this.total;
	}

	public Instant getIssuedAt() {
		return this.issuedAt;
	}

	public Instant getRecordedAt() {
		return this.recordedAt;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		return (other instanceof InvoiceRecord record) && Objects.equals(this.invoiceId, record.invoiceId);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(this.invoiceId);
	}

}
