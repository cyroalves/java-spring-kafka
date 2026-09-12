package com.cyro.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Pipeline de pedidos orientado a eventos.
 *
 * <p>Um único processo Spring Boot hospeda o produtor HTTP e todos os
 * consumidores. Em produção cada consumidor viraria um deployment separado —
 * aqui eles convivem para que o fluxo inteiro caiba em um {@code docker compose up}.
 */
@SpringBootApplication
public class JavaSpringKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(JavaSpringKafkaApplication.class, args);
	}

}
