package com.cyro.kafka.store;

import java.util.List;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Acesso à tabela {@code invoices}.
 */
public interface InvoiceRecordRepository extends JpaRepository<InvoiceRecord, Long> {

	boolean existsByInvoiceId(String invoiceId);

	/**
	 * Faturas de um pedido — {@code List}, e não {@code Optional}, de propósito.
	 *
	 * <p>Parece "uma fatura por pedido", mas não é garantido. O
	 * {@code InvoiceProcessor} gera um {@code invoiceId} novo a cada tentativa; se
	 * um pedido for reprocessado depois de uma transação abortada, o mesmo
	 * {@code orderId} ganha uma segunda fatura — com id diferente, então a
	 * constraint de unicidade não a bloqueia.
	 *
	 * <p>É at-least-once aparecendo no read model. Evitar isso de verdade exigiria
	 * o {@code invoiceId} ser derivado do pedido (por exemplo, UUID v5 do
	 * {@code orderId}) em vez de aleatório — aí a segunda gravação colidiria com a
	 * primeira e seria descartada.
	 */
	List<InvoiceRecord> findByOrderId(String orderId);

	List<InvoiceRecord> findByCustomerIdOrderByIssuedAtDesc(String customerId, Limit limit);

	List<InvoiceRecord> findAllByOrderByIssuedAtDesc(Limit limit);

}
