package com.cyro.kafka.config;

/**
 * Nomes de tópicos em um lugar só.
 *
 * <p>O sufixo {@code .v1} é versionamento de contrato: quando o schema mudar de
 * forma incompatível, cria-se {@code orders.v2} e os dois convivem enquanto os
 * consumidores migram. Renomear campo em tópico existente quebra consumidor em
 * produção — versionar é mais barato que coordenar deploys.
 */
public final class Topics {

	/** Pedidos recebidos pela API HTTP. Chave = customerId. */
	public static final String ORDERS = "orders.v1";

	/**
	 * Sufixo da dead letter queue.
	 *
	 * <p>O {@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer}
	 * tem um sufixo padrão, mas ele <b>mudou entre versões</b> do spring-kafka
	 * ({@code .DLT} até a linha 3.x, {@code -dlt} na 4.x). Depender do padrão faz
	 * um upgrade de versão passar a publicar em um tópico que não existe — e, com
	 * autocriação desligada, o produtor trava em retry de metadata sem erro claro.
	 * Por isso o destino é declarado explicitamente aqui e passado ao recoverer.
	 */
	public static final String DLT_SUFFIX = ".DLT";

	/** Dead letter dos pedidos. */
	public static final String ORDERS_DLT = ORDERS + DLT_SUFFIX;

	/** Faturas emitidas pelo processador transacional. */
	public static final String INVOICES = "invoices.v1";

	private Topics() {
	}

}
