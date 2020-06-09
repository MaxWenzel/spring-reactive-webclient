package org.devzone.webclient;

import org.devzone.webclient.model.Address;
import org.devzone.webclient.service.AddressChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

@SpringBootApplication
public class SpringWebclientApplication implements CommandLineRunner {

	private static final Logger logger = LoggerFactory.getLogger(SpringWebclientApplication.class);

	private final AddressChecker addressChecker;

	public SpringWebclientApplication(AddressChecker addressChecker) {
		Hooks.onOperatorDebug();
		this.addressChecker = addressChecker;
	}

	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = SpringApplication.run(SpringWebclientApplication.class, args);

		// shut down all beans, otherwise the application would keep running
		ctx.close();
	}

	@Override
	public void run(String... args) throws Exception {
		Set<Address> addresses = addressChecker.importAddressData();

		Instant start = Instant.now();
		Flux<Address> addressFlux = addressChecker.fireRequest(addresses);
		//addressFlux.map(a -> a.getLocality()).subscribe(loc -> logger.info("Retrieved locality: {}", loc));

		Address address = addressFlux.blockLast(Duration.ofSeconds(15));
		Instant finish = Instant.now();
		long timeElapsed = Duration.between(start, finish).toSeconds();
		logger.info("Finished after {} seconds", timeElapsed);
	}
}
