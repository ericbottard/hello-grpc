package io.projectriff.invoker.streaming;

import java.io.IOException;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import reactor.core.publisher.Flux;

public class Client {

	public static void main(String[] args) throws InterruptedException, IOException {

		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
				.usePlaintext()
				.overrideAuthority("localhost")
				.build();


		ReactorRiffGrpc.ReactorRiffStub stub = ReactorRiffGrpc.newReactorStub(channel);
		Flux<Signal> req = Flux.just(
				Signal.newBuilder().setStart(Start.newBuilder().setContentType("text/plain").setAccept("text/plain").build()).build(),
				Signal.newBuilder().setNext(Next.newBuilder().setPayload(ByteString.copyFromUtf8("Hello")).build()).build(),
				Signal.newBuilder().setComplete(Complete.newBuilder().build()).build()
		);
		Flux<Signal> resp = stub.invoke(req);
		resp.subscribe(System.out::println);

		System.in.read();

	}
}
