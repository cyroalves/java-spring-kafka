package com.cyro.kafka.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.cyro.kafka.order.NewOrderRequest;
import com.cyro.kafka.order.Order;
import com.cyro.kafka.order.OrderPublisher;

import jakarta.validation.Valid;

/**
 * Porta de entrada HTTP do pipeline.
 *
 * <p>O endpoint devolve <b>202 Accepted</b>, não 200/201: o pedido foi aceito e
 * durabilizado no log do Kafka, mas ainda não foi processado. Responder 201
 * ("created") seria mentir sobre o estado do sistema — o processamento é
 * assíncrono e pode até falhar e cair na DLT depois da resposta.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

	private final OrderPublisher publisher;

	public OrderController(OrderPublisher publisher) {
		this.publisher = publisher;
	}

	/**
	 * Aceita um pedido e publica em {@code orders.v1}.
	 *
	 * <p>O {@code join()} bloqueia até o ack do broker. É uma escolha: troca
	 * latência por garantia. Sem ele a API responderia mais rápido, mas poderia
	 * confirmar ao cliente um pedido que nunca chegou ao log.
	 */
	@PostMapping
	public ResponseEntity<Order> create(@Valid @RequestBody NewOrderRequest request) {
		Order order = new Order(
				UUID.randomUUID().toString(),
				request.customerId(),
				request.amount(),
				Instant.now());

		try {
			this.publisher.publish(order).join();
		}
		catch (CompletionException ex) {
			// O broker não confirmou. 503 em vez de 500: é falha de
			// infraestrutura e faz sentido o cliente tentar de novo.
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
		}

		return ResponseEntity.accepted().body(order);
	}

	/**
	 * Gera uma rajada de pedidos para ver particionamento e consumo em paralelo.
	 *
	 * <p>Cria {@code n} pedidos distribuídos entre poucos clientes, de propósito:
	 * clientes repetidos mostram várias mensagens caindo na mesma partição.
	 */
	@PostMapping("/burst")
	public ResponseEntity<Integer> burst(@RequestParam(defaultValue = "9") int count) {
		for (int i = 0; i < count; i++) {
			Order order = new Order(
					UUID.randomUUID().toString(),
					"customer-" + (i % 3),
					BigDecimal.valueOf(100L + i),
					Instant.now());
			this.publisher.publish(order);
		}
		return ResponseEntity.accepted().body(count);
	}

}
