package com.cyro.kafka.web;

import java.util.List;

/**
 * Um pedido com as faturas que ele gerou.
 *
 * <p>{@code invoices} é lista, e não um campo único, porque um pedido
 * reprocessado depois de uma transação abortada pode render mais de uma fatura —
 * ver {@code InvoiceRecordRepository#findByOrderId}. Esconder isso atrás de um
 * campo só faria a API mentir sobre o que está gravado.
 */
public record OrderDetailView(OrderView order, List<InvoiceView> invoices) {
}
