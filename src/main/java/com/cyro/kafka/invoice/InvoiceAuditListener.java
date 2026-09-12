package com.cyro.kafka.invoice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consome {@code invoices.v1} só para provar o efeito do isolamento.
 *
 * <p>O consumidor roda com {@code isolation.level=read_committed} (definido em
 * application.yml). Por isso faturas de transações abortadas — como a do gatilho
 * de rollback no {@link InvoiceProcessor} — <b>não</b> aparecem aqui, mesmo tendo
 * sido fisicamente escritas no log da partição.
 *
 * <p>Com {@code read_uncommitted} (o padrão do Kafka) elas apareceriam, e o
 * trabalho todo da transação seria inútil do lado do consumidor.
 */
@Component
public class InvoiceAuditListener {

	private static final Logger log = LoggerFactory.getLogger(InvoiceAuditListener.class);

	@KafkaListener(
			topics = "#{T(com.cyro.kafka.config.Topics).INVOICES}",
			groupId = "invoice-audit")
	public void onInvoice(Invoice invoice) {
		log.info("fatura visível (committed): {} pedido {} total {}",
				invoice.invoiceId(), invoice.orderId(), invoice.total());
	}

}
