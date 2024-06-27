package com.nexusforge.reactivekafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.nexusforge.reactivekafka.sec17.${app}")
public class ReactiveKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveKafkaApplication.class, args);
	}

}
