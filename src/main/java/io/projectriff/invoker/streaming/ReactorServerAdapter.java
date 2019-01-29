package io.projectriff.invoker.streaming;

import java.io.IOException;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import reactor.core.publisher.Flux;

import org.springframework.core.ResolvableType;
import org.springframework.core.codec.ByteArrayDecoder;
import org.springframework.core.codec.ByteArrayEncoder;
import org.springframework.core.codec.Decoder;
import org.springframework.core.codec.Encoder;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.util.MimeType;

public class ReactorServerAdapter extends ReactorRiffGrpc.RiffImplBase {

	private final Function fn;

	public ReactorServerAdapter(Function fn) {
		this.fn = fn; // Function<Flux<String> -> Flux<Integer>>
	}

	// RiffSignal -> demat -Flux"Propre"<RiffSignal>--> decode() -Flux<T == String> -> fn()
	// --Flux<Integer>-> encode() -Flux<Buffer>-> Flux<RiffSignal(bytes)> -> materialize ->
	// Flux<Signal=N,C,E>
	@Override
	public Flux<Signal> invoke(Flux<Signal> request) {

		// reactor -> 3.2.5 => switchOnFirst()
//		return request.switchOnFirst((signal, r2) -> {
//			System.out.println("WOOOOOOOOOOOOOOOOOOOOOO");
//			if (signal.hasValue()) {

				Flux<Signal> r2 = request;

				Decoder decoder = new ByteArrayDecoder();
				MimeType contentType = MimeType.valueOf("text/plain");

				Encoder encoder = new ByteArrayEncoder();
				MimeType accept = MimeType.valueOf("text/plain");

				DataBufferFactory dbf = new DefaultDataBufferFactory();

				Flux<Signal> riffSignalsAsFlux = r2.log().map(ReactorServerAdapter::toReactorSignal)
						.dematerialize();

				Flux<byte[]> userData = decoder.decode(
						riffSignalsAsFlux.log().map(s -> dbf.wrap(s.getNext().getPayload().asReadOnlyByteBuffer())),
						ResolvableType.forClass(byte[].class), contentType,
						null);

				// userData.checkpoint().subscribe(d -> System.out.println("WTF " + d.getClass()),
				// Throwable::printStackTrace);
				Flux<?> result = userData.transform(fn);

				return encoder
						.encode(result, dbf, ResolvableType.forClass(byte[].class), accept, null).log()
						.materialize().map(ReactorServerAdapter::toRiffSignal);
//			}
//			else {
//				return Flux.error(new RuntimeException("Missing Start frame"));
//			}
//		});

	}

	private static reactor.core.publisher.Signal<Signal> toReactorSignal(Signal signal) {
		switch (signal.getValueCase()) {
		case NEXT:
			return reactor.core.publisher.Signal.next(signal);
		case ERROR:
			return reactor.core.publisher.Signal.error(new RuntimeException("Need to decide how to convert"));
		case COMPLETE:
			return reactor.core.publisher.Signal.complete();
		default:
			throw new RuntimeException("Unexpected riff signal type: " + signal);
		}
	}

	private static Signal toRiffSignal(Object s) {
		reactor.core.publisher.Signal<DataBuffer> signal = (reactor.core.publisher.Signal<DataBuffer>) s;
		switch (signal.getType()) {
		case ON_NEXT:
			try {
				return Signal.newBuilder().setNext(
						Next.newBuilder().setPayload(ByteString.readFrom(signal.get().asInputStream(true)))).build();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		case ON_COMPLETE:
			return Signal.newBuilder().setComplete(Complete.newBuilder().build()).build();
		case ON_ERROR:
			return Signal.newBuilder().setError(Error.newBuilder().build()).build();
		default:
			throw new RuntimeException("Unexpected riff signal type: " + signal);
		}
	}

}
