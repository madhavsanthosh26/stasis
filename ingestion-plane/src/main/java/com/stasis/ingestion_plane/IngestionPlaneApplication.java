package com.stasis.ingestion_plane;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling 
@SpringBootApplication
public class IngestionPlaneApplication {

	public static void main(String[] args) {
		SpringApplication.run(IngestionPlaneApplication.class, args);
	}

}
