package com.cyro.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Cria os tópicos no start.
 *
 * <p>O Boot registra um {@code KafkaAdmin} e, ao subir, aplica todo bean
 * {@link NewTopic} encontrado no contexto. Se o tópico já existe, ele é deixado
 * como está (partições podem ser aumentadas, nunca reduzidas).
 *
 * <p>Declarar tópicos em código existe porque o broker está com
 * {@code auto.create.topics.enable=false}. Autocriação é confortável e traiçoeira:
 * um typo no nome do tópico cria um tópico novo com 1 partição e configuração
 * padrão, e o bug só aparece em carga.
 */
@Configuration
public class KafkaTopicConfig {

	/**
	 * Três partições = até três consumidores em paralelo no mesmo grupo.
	 *
	 * <p>O número de partições é o teto do paralelismo de um grupo. Subir depois é
	 * possível, mas muda o {@code hash(key) % partições} e portanto <b>quebra a
	 * garantia de ordem</b> para chaves já existentes — por isso se escolhe com
	 * folga desde o início.
	 */
	@Bean
	NewTopic ordersTopic() {
		return TopicBuilder.name(Topics.ORDERS)
				.partitions(3)
				.replicas(1)
				.build();
	}

	/**
	 * DLT com 3 partições também: o recoverer publica o registro morto na
	 * <b>mesma partição</b> de origem, então a DLT precisa ter pelo menos a mesma
	 * quantidade — senão ele cai no roteamento padrão.
	 */
	@Bean
	NewTopic ordersDltTopic() {
		return TopicBuilder.name(Topics.ORDERS_DLT)
				.partitions(3)
				.replicas(1)
				.build();
	}

	@Bean
	NewTopic invoicesTopic() {
		return TopicBuilder.name(Topics.INVOICES)
				.partitions(3)
				.replicas(1)
				.build();
	}

	/**
	 * DLT das faturas — mesmo número de partições, mesma razão.
	 *
	 * <p>Só passou a ser alcançável quando o {@code InvoiceStoreListener} começou
	 * a gravar no Postgres: até então nenhum consumidor de {@code invoices.v1}
	 * podia falhar. Declarar um tópico que talvez nunca receba nada é barato;
	 * descobrir que ele falta é caro, porque o sintoma é um produtor travado em
	 * retry de metadata, sem exceção.
	 */
	@Bean
	NewTopic invoicesDltTopic() {
		return TopicBuilder.name(Topics.INVOICES_DLT)
				.partitions(3)
				.replicas(1)
				.build();
	}

}
