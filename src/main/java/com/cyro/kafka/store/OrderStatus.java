package com.cyro.kafka.store;

/**
 * Situação de um pedido no read model.
 *
 * <p>Gravado como texto na coluna {@code status}, com {@code CHECK} no banco.
 * Texto e não {@code ordinal}: {@link jakarta.persistence.EnumType#ORDINAL} grava
 * a posição da constante, então inserir um valor novo no meio do enum reescreve
 * o significado de todas as linhas já gravadas — sem erro, sem aviso.
 */
public enum OrderStatus {

	/** Passou nas regras de {@code OrderRules} e seguiu no fluxo. */
	APPROVED,

	/** Reprovado nas regras — o mesmo motivo que o manda para a DLT. */
	REJECTED

}
