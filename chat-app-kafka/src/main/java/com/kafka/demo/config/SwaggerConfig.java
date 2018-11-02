package com.kafka.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafka.demo.services.KafkaProducer;

import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;
import static springfox.documentation.builders.PathSelectors.regex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Configuration
@EnableSwagger2
public class SwaggerConfig {
	private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

	@Bean
	public Docket productApi() {
	    log.info("XYZ: SwaggerConfig.productApi");

		return new Docket(DocumentationType.SWAGGER_2).select()
				.apis(RequestHandlerSelectors.basePackage("com.kafka.demo.controller"))
				.paths(regex("/kafka/chat.*")).build();
	}
}