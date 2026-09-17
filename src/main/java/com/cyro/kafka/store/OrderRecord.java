package com.cyro.kafka.store;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Linha da tabela {@code orders} — a projeção de um evento de {@code orders.v1}.
 *
 * <p>Entidade separada do record {@link com.cyro.kafka.order.Order} pelo mesmo
 * motivo que {@code NewOrderRequest} é separado dele: são três contratos com
 * ritmos de mudança diferentes (HTTP, evento, tabela). Usar a mesma classe nos
 * três faz uma anotação de JPA vazar para o payload do Kafka e uma coluna nova
 * virar mudança de schema de mensagem.
 *
 * <p><b>Somente inserção.</b> Nenhum campo tem setter: uma linha aqui é o
 * registro de um evento que já aconteceu, e evento não se edita. Isso também
 * dispensa {@code @Version} — sem update concorrente não há o que travar.
 */
@Entity
@Table(name = "orders")
public class OrderRecord {

	/**
	 * Chave primária técnica, gerada por {@code SEQUENCE}.
	 *
	 * <p>Não é {@link GenerationType#IDENTITY} de propósito. {@code IDENTITY}
	 * obriga o Hibernate a executar o {@code INSERT} na hora do {@code persist()}
	 * para saber a chave que o banco gerou — e com isso o batch de inserts nunca
	 * se forma. Com {@code SEQUENCE} ele reserva 50 ids de uma vez e pode agrupar.
	 *
	 * <p>{@code allocationSize} tem que bater com o {@code INCREMENT BY} da
	 * sequence no V1. Divergir não dá erro no start: dá violação de chave
	 * primária mais tarde, quando duas instâncias reservarem faixas sobrepostas.
	 */
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "order_record_seq")
	@SequenceGenerator(name = "order_record_seq", sequenceName = "order_record_seq", allocationSize = 50)
	private Long id;

	/**
	 * Chave natural, vinda do evento.
	 *
	 * <p>O {@code UNIQUE} na coluna é a idempotência do consumidor. A entrega do
	 * Kafka é at-least-once: em rebalance ou retry a mesma mensagem volta, e o
	 * banco recusa a segunda gravação. O {@code if} no serviço evita o custo da
	 * exceção no caso comum, mas quem garante é a constraint.
	 */
	@Column(name = "order_id", nullable = false, unique = true, length = 36)
	private String orderId;

	@Column(name = "customer_id", nullable = false, length = 64)
	private String customerId;

	@Column(name = "amount", nullable = false, precision = 19, scale = 2)
	private BigDecimal amount;

	@Enumerated(EnumType.STRING)
	@Column(name = "status", nullable = false, length = 16)
	private OrderStatus status;

	/** Quando a API aceitou o pedido — vem no evento. */
	@Column(name = "created_at", nullable = false)
	private Instant createdAt;

	/** Quando este consumidor gravou a linha. A diferença para {@code createdAt} é o lag. */
	@Column(name = "recorded_at", nullable = false)
	private Instant recordedAt;

	/** Exigido pelo JPA; {@code protected} para não virar construtor de uso geral. */
	protected OrderRecord() {
	}

	public OrderRecord(String orderId, String customerId, BigDecimal amount,
			OrderStatus status, Instant createdAt, Instant recordedAt) {
		this.orderId = orderId;
		this.customerId = customerId;
		this.amount = amount;
		this.status = status;
		this.createdAt = createdAt;
		this.recordedAt = recordedAt;
	}

	public Long getId() {
		return this.id;
	}

	public String getOrderId() {
		return this.orderId;
	}

	public String getCustomerId() {
		return this.customerId;
	}

	public BigDecimal getAmount() {
		return this.amount;
	}

	public OrderStatus getStatus() {
		return this.status;
	}

	public Instant getCreatedAt() {
		return this.createdAt;
	}

	public Instant getRecordedAt() {
		return this.recordedAt;
	}

	/**
	 * Igualdade pela chave <b>natural</b>, não pela gerada.
	 *
	 * <p>Antes do flush o {@code id} ainda é {@code null}, então uma entidade nova
	 * seria igual a qualquer outra entidade nova — e duas delas em um
	 * {@code HashSet} viram uma só. O {@code orderId} existe desde a construção.
	 */
	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		return (other instanceof OrderRecord record) && Objects.equals(this.orderId, record.orderId);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(this.orderId);
	}

}
