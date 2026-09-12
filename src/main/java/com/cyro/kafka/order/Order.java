package com.cyro.kafka.order;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento de pedido — o contrato que trafega no tópico {@code orders.v1}.
 *
 * <p>É um {@code record} imutável de propósito: uma mensagem que já foi publicada
 * não pode mudar, e o objeto que a representa também não deveria.
 *
 * <p>Só campos com getter público entram no JSON. Note que o nome da classe
 * <em>não</em> vai no payload: o mapeamento lógico {@code order -> Order} é
 * declarado em {@code spring.json.type.mapping} (ver application.yml), então o
 * consumidor não precisa ter a mesma package do produtor.
 *
 * @param orderId    identificador único do pedido (idempotência do consumidor)
 * @param customerId cliente dono do pedido — é a <b>chave</b> da mensagem
 * @param amount     valor total
 * @param createdAt  momento em que a API aceitou o pedido
 */
public record Order(
		String orderId,
		String customerId,
		BigDecimal amount,
		Instant createdAt) {
}
