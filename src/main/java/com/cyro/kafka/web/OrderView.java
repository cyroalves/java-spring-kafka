package com.cyro.kafka.web;

import java.math.BigDecimal;
import java.time.Instant;

import com.cyro.kafka.store.OrderRecord;
import com.cyro.kafka.store.OrderStatus;

/**
 * Como um pedido gravado aparece na API.
 *
 * <p>A entidade JPA não é serializada direto de propósito. Devolver
 * {@code OrderRecord} acoplaria o JSON ao schema da tabela — renomear coluna
 * viraria mudança de contrato HTTP — e exporia o {@code id} técnico, que não
 * significa nada fora do banco.
 *
 * @param recordedAt quando o consumidor gravou; a distância para {@code createdAt}
 * é o lag do pipeline, e é informação útil para quem consulta
 */
public record OrderView(
		String orderId,
		String customerId,
		BigDecimal amount,
		OrderStatus status,
		Instant createdAt,
		Instant recordedAt) {

	public static OrderView from(OrderRecord record) {
		return new OrderView(
				record.getOrderId(),
				record.getCustomerId(),
				record.getAmount(),
				record.getStatus(),
				record.getCreatedAt(),
				record.getRecordedAt());
	}

}
