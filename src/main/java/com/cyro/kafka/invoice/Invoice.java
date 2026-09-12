package com.cyro.kafka.invoice;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Fatura emitida a partir de um pedido — contrato do tópico {@code invoices.v1}.
 *
 * @param invoiceId identificador da fatura
 * @param orderId   pedido que originou a fatura (rastreabilidade)
 * @param customerId cliente — mantido como chave para preservar a ordem por cliente
 * @param total     valor com imposto aplicado
 * @param issuedAt  momento da emissão
 */
public record Invoice(
		String invoiceId,
		String orderId,
		String customerId,
		BigDecimal total,
		Instant issuedAt) {
}
