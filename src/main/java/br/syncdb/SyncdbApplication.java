package br.syncdb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SyncdbApplication {

	public static void main(String[] args) {
		SpringApplication.run(SyncdbApplication.class, args);
		System.out.println("Olá Mundo");
	}

}
