package com.stratio.pg2kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.config.EnableIntegration;

@SpringBootApplication
@EnableIntegration
public class Pg2kafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(Pg2kafkaApplication.class, args);
	}

}

