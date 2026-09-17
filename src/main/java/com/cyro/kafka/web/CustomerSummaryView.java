package com.cyro.kafka.web;

import java.math.BigDecimal;
import java.util.List;

/**
 * O que o read model responde e o tópico não responderia.
 *
 * <p>"Quanto este cliente tem aprovado, e quais foram os últimos pedidos" exige
 * varrer e agregar. Em Kafka isso significaria reler a partição inteira a cada
 * pergunta; em SQL é um {@code sum} com índice. É a justificativa da tabela
 * existir — não guardar o evento de novo, mas responder perguntas de outra forma.
 */
public record CustomerSummaryView(
		String customerId,
		BigDecimal approvedTotal,
		List<OrderView> orders,
		List<InvoiceView> invoices) {
}
