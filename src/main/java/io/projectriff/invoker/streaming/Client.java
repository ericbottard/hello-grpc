package io.projectriff.invoker.streaming;

import java.io.IOException;
import java.time.Duration;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import reactor.core.publisher.Flux;

public class Client {

	public static void main(String[] args) throws InterruptedException, IOException {

//		ManagedChannel channel = ManagedChannelBuilder.forAddress("35.241.225.245", 80)
//				.usePlaintext()
//				.overrideAuthority("grpc.default.example.com")
//				.build();
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
				.usePlaintext()
				.overrideAuthority("localhost")
				.build();


		ReactorRiffGrpc.ReactorRiffStub stub = ReactorRiffGrpc.newReactorStub(channel);
		Signal start = Signal.newBuilder().setStart(Start.newBuilder().setContentType("text/plain").setAccept("text/plain").build()).build();
		Flux<Signal> req = Flux.just(
				start,
//				Signal.newBuilder().setNext(Next.newBuilder().setPayload(ByteString.copyFromUtf8("Hello")).putHeaders("Content-Type", "text/plain").build()).build(),
//				Signal.newBuilder().setNext(Next.newBuilder().setPayload(ByteString.copyFromUtf8("\"riff\"")).putHeaders("Content-Type", "application/json").build()).build()
				Signal.newBuilder().setNext(Next.newBuilder().setPayload(ByteString.copyFromUtf8("4")).putHeaders("Content-Type", "text/plain").build()).build(),
				Signal.newBuilder().setNext(Next.newBuilder().setPayload(ByteString.copyFromUtf8("8")).putHeaders("Content-Type", "application/json").build()).build()
				//Signal.newBuilder().setComplete(Complete.newBuilder().build()).build()
		);

		Flux<Signal> numbers = Flux.interval(Duration.ofMillis(2000)).map(i -> i + 1).doOnNext(System.out::println)
				.map(i -> Signal.newBuilder().setNext(Next.newBuilder().setPayload(ByteString.copyFromUtf8("" + i))
						.putHeaders("Content-Type", "text/plain")).build());
		Flux<Signal> resp = stub.invoke(Flux.just(start).concatWith( numbers));
		resp.subscribe(System.out::println);

		System.in.read();

	}
}
