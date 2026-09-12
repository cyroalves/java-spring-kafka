package com.cyro.kafka.order;

/**
 * Falha <b>permanente</b>: o pedido está errado e vai continuar errado.
 *
 * <p>Registrada em {@code addNotRetryableExceptions} — o
 * {@link org.springframework.kafka.listener.DefaultErrorHandler} pula o backoff
 * e manda direto para a DLT. Repetir uma mensagem inválida dez vezes só atrasa
 * a partição inteira; o defeito está no dado, não no ambiente.
 */
public class InvalidOrderException extends RuntimeException {

	public InvalidOrderException(String message) {
		super(message);
	}

}
