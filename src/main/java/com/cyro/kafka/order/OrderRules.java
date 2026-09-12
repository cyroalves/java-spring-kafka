package com.cyro.kafka.order;

import java.math.BigDecimal;

/**
 * As regras que decidem se um pedido é aceitável.
 *
 * <p>Ficam fora dos listeners porque <b>dois</b> consumidores precisam delas, em
 * grupos diferentes, sobre o mesmo tópico.
 *
 * <p>Em um sistema real o faturamento não reavaliaria o pedido: a validação
 * publicaria {@code orders.validated.v1} e o faturamento consumiria <em>esse</em>
 * tópico, formando uma cadeia. Aqui os dois consomem {@code orders.v1} de
 * propósito, para demonstrar o fan-out entre grupos — e, para o resultado não
 * ficar incoerente (um pedido rejeitado virando fatura), ambos aplicam a mesma
 * regra.
 */
public final class OrderRules {

	/** Acima disso o pedido exige aprovação manual e não segue no fluxo. */
	public static final BigDecimal MAX_AMOUNT = new BigDecimal("10000");

	/**
	 * Lança {@link InvalidOrderException} se o pedido for inaceitável.
	 *
	 * <p>Exceção e não {@code boolean}: é ela que o {@code DefaultErrorHandler}
	 * classifica como não-retriável para mandar direto à DLT.
	 */
	public static void validate(Order order) {
		if (order.amount() == null || order.amount().signum() <= 0) {
			throw new InvalidOrderException("amount deve ser positivo: " + order.amount());
		}
		if (order.amount().compareTo(MAX_AMOUNT) > 0) {
			throw new InvalidOrderException("amount acima do limite: " + order.amount());
		}
	}

	/** Versão sem exceção, para quem só precisa decidir se processa. */
	public static boolean isAcceptable(Order order) {
		try {
			validate(order);
			return true;
		}
		catch (InvalidOrderException ex) {
			return false;
		}
	}

	private OrderRules() {
	}

}
