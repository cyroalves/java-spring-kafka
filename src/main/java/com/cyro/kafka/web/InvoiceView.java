package com.cyro.kafka.web;

import java.math.BigDecimal;
import java.time.Instant;

import com.cyro.kafka.store.InvoiceRecord;

/** Como uma fatura gravada aparece na API. */
public record InvoiceView(
		String invoiceId,
		String orderId,
		String customerId,
		BigDecimal total,
		Instant issuedAt,
		Instant recordedAt) {

	public static InvoiceView from(InvoiceRecord record) {
		return new InvoiceView(
				record.getInvoiceId(),
				record.getOrderId(),
				record.getCustomerId(),
				record.getTotal(),
				record.getIssuedAt(),
				record.getRecordedAt());
	}

}
