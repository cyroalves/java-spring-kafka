package com.cyro.kafka.order;

/**
 * Falha <b>temporária</b>: banco fora do ar, timeout, 503 de um serviço vizinho.
 *
 * <p>Vale a pena repetir com backoff — a mensagem provavelmente é boa e só o
 * ambiente estava ruim. Se depois de todas as tentativas ainda falhar, aí sim
 * vai para a DLT.
 */
public class TransientFailureException extends RuntimeException {

	public TransientFailureException(String message) {
		super(message);
	}

}
