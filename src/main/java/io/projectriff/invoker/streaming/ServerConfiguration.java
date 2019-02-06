package io.projectriff.invoker.streaming;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Function;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import reactor.core.publisher.Flux;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistration;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.FunctionalSpringApplication;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
public class ServerConfiguration {

	public static void main(String[] args) throws InterruptedException, IOException {
//		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ServerConfiguration.class);
		FunctionalSpringApplication fsa = new FunctionalSpringApplication(ServerConfiguration.class);
		ConfigurableApplicationContext context = fsa.run();
		System.in.read();
	}

	@Bean(initMethod = "start", destroyMethod = "shutdown")
	public Server server(FunctionInspector fi, FunctionRegistry ignored) {
		return ServerBuilder.forPort(8080).addService(invokerAdapter(fi)).build();
	}

	@Bean
	public BindableService invokerAdapter(FunctionInspector fi) {
		return new ReactorServerThirdAdapter(otherFunction(), fi);
	}

	@Bean
	public Function<Flux<String>, Flux<Integer>> userFunction() {
		return f -> f.map(String::length);
	}

	@Bean
	public Function<Flux<Integer>, Flux<Double>> otherFunction() {
		return is -> is.window(Duration.ofMillis(6000))
				.flatMap(w -> w.reduce(0D, (a, b) -> a+b));
	}
}
