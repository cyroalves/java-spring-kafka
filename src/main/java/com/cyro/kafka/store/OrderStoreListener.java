package com.cyro.kafka.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.cyro.kafka.order.Order;

/**
 * Projeta {@code orders.v1} na tabela {@code orders}.
 *
 * <p>Terceiro grupo de consumo sobre o mesmo tópico, ao lado de
 * {@code order-validation} e {@code invoicing}. Podia ser uma linha dentro do
 * {@code OrderValidationListener} — e não é, por duas razões:
 *
 * <ul>
 *   <li>o listener de validação falha de propósito (retry, backoff, DLT); a
 *       gravação não deveria depender do orçamento de tentativas dele;</li>
 *   <li>grupos separados têm offsets separados. Se a projeção precisar ser
 *       reconstruída, basta resetar o offset <em>deste</em> grupo e reler o
 *       tópico. Com o código junto, reler significaria revalidar tudo.</li>
 * </ul>
 *
 * <p>É o que um log particionado permite e uma fila não: o mesmo evento
 * alimentando consumidores independentes, cada um com seu próprio ponteiro.
 */
@Component
public class OrderStoreListener {

	private static final Logger log = LoggerFactory.getLogger(OrderStoreListener.class);

	private final ReadModelStore store;

	public OrderStoreListener(ReadModelStore store) {
		this.store = store;
	}

	@KafkaListener(
			topics = "#{T(com.cyro.kafka.config.Topics).ORDERS}",
			groupId = "order-store")
	public void onOrder(Order order) {
		try {
			if (this.store.save(order)) {
				log.info("pedido {} gravado no read model", order.orderId());
			}
			else {
				// Reentrega: o offset não avançou antes de um rebalance ou de um
				// restart. Esperado em at-least-once, não é erro.
				log.debug("pedido {} já estava gravado — reentrega ignorada", order.orderId());
			}
		}
		catch (DataIntegrityViolationException ex) {
			// Duas instâncias inserindo o mesmo orderId ao mesmo tempo. Quem
			// perdeu a corrida chega aqui, e o resultado final é o mesmo: uma
			// linha só. Relançar mandaria uma mensagem perfeitamente processada
			// para a DLT.
			log.debug("pedido {} inserido concorrentemente — nada a fazer", order.orderId());
		}
		// Qualquer outra falha (banco fora do ar, timeout de conexão) sobe: o
		// DefaultErrorHandler a trata como temporária, repete com backoff e, se
		// não resolver, manda o pedido para orders.v1.DLT.
	}

}
