package com.cyro.kafka.store;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Acesso à tabela {@code orders}.
 *
 * <p>Interface sem implementação: o Spring Data gera o bean em tempo de execução
 * e traduz o <b>nome</b> de cada método em JPQL. É conveniente e tem um custo —
 * um erro de digitação em {@code findByCustumerId} não compila em SQL inválido,
 * quebra o start da aplicação com "No property 'custumerId' found". Falhar no
 * start é o comportamento desejado; saber que é assim que se manifesta, também.
 */
public interface OrderRecordRepository extends JpaRepository<OrderRecord, Long> {

	/** Checagem barata antes de gravar. Quem garante a unicidade é a constraint. */
	boolean existsByOrderId(String orderId);

	Optional<OrderRecord> findByOrderId(String orderId);

	/**
	 * Últimos pedidos de um cliente.
	 *
	 * <p>{@link Limit} em vez de {@code Pageable}: o caso de uso é "os N mais
	 * recentes", e {@code Pageable} traria uma contagem total que ninguém pediu.
	 */
	List<OrderRecord> findByCustomerIdOrderByCreatedAtDesc(String customerId, Limit limit);

	List<OrderRecord> findAllByOrderByCreatedAtDesc(Limit limit);

	/**
	 * Total aprovado por cliente.
	 *
	 * <p>JPQL escrito à mão porque agregação não cabe em nome de método. Note que
	 * a consulta fala de <em>entidades</em> ({@code OrderRecord}, {@code status}),
	 * não de tabelas e colunas — é JPQL, não SQL.
	 *
	 * <p>{@code coalesce} porque {@code sum} de conjunto vazio devolve
	 * {@code null}, não zero.
	 */
	@Query("""
			select coalesce(sum(o.amount), 0)
			from OrderRecord o
			where o.customerId = :customerId and o.status = com.cyro.kafka.store.OrderStatus.APPROVED
			""")
	BigDecimal sumApprovedAmount(@Param("customerId") String customerId);

}
