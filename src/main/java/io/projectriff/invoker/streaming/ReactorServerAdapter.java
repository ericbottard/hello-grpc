package io.projectriff.invoker.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

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
import org.springframework.http.codec.support.DefaultServerCodecConfigurer;
import org.springframework.util.MimeType;

public class ReactorServerAdapter extends ReactorRiffGrpc.RiffImplBase {

	private final Function fn;

	private DataBufferFactory dbf = new DefaultDataBufferFactory();

	private List<Encoder> encoders = new ArrayList<>();
	private List<Decoder> decoders = new ArrayList<>();

	public ReactorServerAdapter(Function fn) {
		this.fn = fn; // Function<Flux<String> -> Flux<Integer>>


		encoders.add(new ByteArrayEncoder());
		decoders.add(new ByteArrayDecoder());
	}

	// RiffSignal -> demat -Flux"Propre"<RiffSignal>--> decode() -Flux<T == String> -> fn()
	// --Flux<Integer>-> encode() -Flux<Buffer>-> Flux<RiffSignal(bytes)> -> materialize ->
	// Flux<Signal=N,C,E>
	@Override
	public Flux<Signal> invoke(Flux<Signal> request) {
		Decoder decoder = new ByteArrayDecoder();
		Encoder encoder = new ByteArrayEncoder();

		return Flux.defer(() -> {
			AtomicReference<Marshalling> marshalling = new AtomicReference<>();

			Flux<Signal> riffSignalsAsFlux = request
					.filter(setAndCheckMarshalling(marshalling)).log()
					.map(ReactorServerAdapter::toReactorSignal)
					.dematerialize();

			Flux<byte[]> userData = decoder.decode(
					riffSignalsAsFlux.log().map(this::convertToDataBuffer),
					ResolvableType.forClass(byte[].class), marshalling.get().contentType,
					null);

			Flux<?> result = userData.transform(fn);

			return encoder
					.encode(result, dbf, ResolvableType.forClass(byte[].class), marshalling.get().accept, null).log()
					.materialize()
					.map(ReactorServerAdapter::toRiffSignal);

		});

	}

	private DataBuffer convertToDataBuffer(Signal s) {
		return dbf.wrap(s.getNext().getPayload().asReadOnlyByteBuffer());
	}

	private Predicate<Signal> setAndCheckMarshalling(AtomicReference<Marshalling> marshalling) {
		return s -> {
			if (s.hasStart() && marshalling.get() == null) {
				Marshalling m = new Marshalling(MimeType.valueOf(s.getStart().getContentType()),
						MimeType.valueOf(s.getStart().getAccept()));
				marshalling.set(m);
				return false;
			}
			else if (!s.hasStart() && marshalling.get() != null) {
				return true;
			}
			else if (s.hasStart() && marshalling.get() != null) {
				throw new RuntimeException("Multiple Start signals seen");
			}
			else {
				throw new RuntimeException("Multiple Start signals seen");
			}
		};
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

	private static class Marshalling {
		final MimeType contentType;

		final MimeType accept;

		private Marshalling(MimeType contentType, MimeType accept) {
			this.contentType = contentType;
			this.accept = accept;
		}
	}

}
