package com.cyro.kafka.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.cyro.kafka.invoice.Invoice;

/**
 * Projeta {@code invoices.v1} na tabela {@code invoices}.
 *
 * <p>O detalhe que faz este consumidor valer a pena: ele roda com
 * {@code isolation.level=read_committed}, então <b>fatura de transação abortada
 * nunca chega ao banco</b>. O {@code make rollback} escreve a fatura fisicamente
 * no log da partição e depois aborta; ela continua lá, invisível, e o
 * {@code SELECT} nesta tabela não a encontra.
 *
 * <p>Com {@code read_uncommitted} — o padrão do Kafka, não desta aplicação — a
 * linha seria gravada e o rollback do produtor não teria efeito prático nenhum
 * no read model. É a diferença entre a transação garantir algo e ser decoração.
 */
@Component
public class InvoiceStoreListener {

	private static final Logger log = LoggerFactory.getLogger(InvoiceStoreListener.class);

	private final ReadModelStore store;

	public InvoiceStoreListener(ReadModelStore store) {
		this.store = store;
	}

	@KafkaListener(
			topics = "#{T(com.cyro.kafka.config.Topics).INVOICES}",
			groupId = "invoice-store")
	public void onInvoice(Invoice invoice) {
		try {
			if (this.store.save(invoice)) {
				log.info("fatura {} gravada no read model (pedido {})",
						invoice.invoiceId(), invoice.orderId());
			}
			else {
				log.debug("fatura {} já estava gravada — reentrega ignorada", invoice.invoiceId());
			}
		}
		catch (DataIntegrityViolationException ex) {
			log.debug("fatura {} inserida concorrentemente — nada a fazer", invoice.invoiceId());
		}
	}

}
