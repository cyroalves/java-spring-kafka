package com.cyro.kafka.order;

import java.math.BigDecimal;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Corpo do POST /api/orders.
 *
 * <p>Propositalmente separado de {@link Order}: o contrato da API HTTP e o
 * contrato do evento evoluem em ritmos diferentes. Juntar os dois em uma classe
 * só significa que mudar a API quebra consumidores Kafka — e vice-versa.
 */
public record NewOrderRequest(
		@NotBlank(message = "customerId é obrigatório")
		String customerId,

		@NotNull(message = "amount é obrigatório")
		@DecimalMin(value = "0.01", message = "amount deve ser positivo")
		BigDecimal amount) {
}
