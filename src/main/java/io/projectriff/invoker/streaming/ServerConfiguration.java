package io.projectriff.invoker.streaming;

import java.io.IOException;
import java.util.function.Function;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import reactor.core.publisher.Flux;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServerConfiguration {

	public static void main(String[] args) throws InterruptedException, IOException {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ServerConfiguration.class);
		System.in.read();
	}

	@Bean(initMethod = "start", destroyMethod = "shutdown")
	public Server server() {
		return ServerBuilder.forPort(8080).addService(invokerAdapter()).build();
	}

	@Bean
	public BindableService invokerAdapter() {
		return new ReactorServerAdapter(userFunction());
	}

	@Bean
	public Function<Flux<byte[]>, Flux<byte[]>> userFunction() {
		return f -> f.map(bs -> ("" + new String(bs).length()).getBytes());
	}
}
