package org.devzone.webclient.service;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.devzone.webclient.model.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.springframework.http.MediaType.APPLICATION_JSON;

@Service
public class AddressChecker {

    private static final Logger logger = LoggerFactory.getLogger(AddressChecker.class);

    private final ResourceLoader loader;
    private final WebClient client;

    public AddressChecker(ResourceLoader loader) {
        this.loader = loader;

        TcpClient tcpClient = TcpClient
                .create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .doOnConnected(connection -> {
                    connection.addHandlerLast(new ReadTimeoutHandler(5000, TimeUnit.MILLISECONDS));
                    connection.addHandlerLast(new WriteTimeoutHandler(5000, TimeUnit.MILLISECONDS));
                });

        client = WebClient
                .builder()
                .clientConnector(new ReactorClientHttpConnector(HttpClient.from(tcpClient)))
                .baseUrl("http://localhost:8082")
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    public Flux<Address> fireRequest(Set<Address> addresses) {
        return fetchAddressesParallel(addresses);
    }

    public Flux<Address> fetchAddressesParallel(Set<Address> addresses) {
        return Flux.fromIterable(addresses)
                .parallel()
                .runOn(Schedulers.elastic())
                .flatMap(a -> findAddress(a.getPostalCode()))
                .ordered((u1, u2) -> u2.getPostalCode().compareTo(u1.getPostalCode()))
                ;
    }

    public Flux<Address> fetchAddresses(Set<Address> addresses) {
        return Flux.fromIterable(addresses)
                .flatMap(a -> findAddress(a.getPostalCode()))
                ;
    }

    public Flux<Address> findAddress(String postalCode) {
        return client.get()
                .uri("/postalcodes/{postalcode}", postalCode)
                .accept(APPLICATION_JSON)
                .retrieve()
                .bodyToFlux(Address.class);
    }

    public Set<Address> importAddressData() {
        Resource addressData = loader.getResource("classpath:postalcode_locality_de.csv");
        Set<Address> addresses = new HashSet<>();
        try {
            try (Stream<String> stream = toLineStream(addressData)) {
                stream.forEach(csvLine -> {
                    try {
                        logger.debug("Read line {}", csvLine);
                        String[] parts = csvLine.split(",");
                        addresses.add(new Address(parts[2], parts[1], parts[3]));
                    } catch (Exception ex) {
                        throw new RuntimeException("Cannot parse the csv line", ex);
                    }
                });
            }
        } catch (Exception ex) {
            logger.error("Error occurred while processing input data", ex);
        }
        return addresses;
    }

    private static Stream<String> toLineStream(Resource resource) throws IOException {
        return new BufferedReader(new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)).lines();
    }

}
