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
import org.springframework.core.codec.StringDecoder;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.http.codec.json.Jackson2JsonDecoder;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.http.codec.support.DefaultServerCodecConfigurer;
import org.springframework.util.MimeType;

public class ReactorServerAdapter extends ReactorRiffGrpc.RiffImplBase {

	private final Function fn;

	private final ResolvableType fnInputType;
	private final ResolvableType fnOutputType;

	private DataBufferFactory dbf = new DefaultDataBufferFactory();

	private List<Encoder> encoders = new ArrayList<>();

	private List<Decoder> decoders = new ArrayList<>();

	public ReactorServerAdapter(Function fn) {
		this.fn = fn;
		this.fnInputType = ResolvableType.forClass(String.class);
		this.fnOutputType = ResolvableType.forClass(Integer.class);

		decoders.add(new Jackson2JsonDecoder());
		decoders.add(StringDecoder.textPlainOnly());
		decoders.add(new ByteArrayDecoder());

		encoders.add(new ByteArrayEncoder());
		encoders.add(new Jackson2JsonEncoder());
	}

	@Override
	public Flux<Signal> invoke(Flux<Signal> request) {
		AtomicReference<Marshalling> marshalling = new AtomicReference<>();

		return request.groupBy(s -> s.hasStart()).concatMap(ks -> {
			if (ks.key()) {
				// mute
				return ks.log("START").single() // Make sure there is exactly 1
						.doOnNext(s -> marshalling.set(new Marshalling( // Mutatate as side-effect
								MimeType.valueOf(s.getStart().getContentType()),
								MediaType.valueOf(s.getStart().getAccept())
						)))
						.ignoreElement(); // Don't propagate to flatMap()
			} else {
				if (marshalling.get() == null) throw new RuntimeException("Should have seen Start Signal by now");
				Flux<Signal> riffSignalsAsFlux = ks.log("OTHER")
						//.doOnNext(s -> {if (marshalling.get() == null) throw new RuntimeException("Ooops");})
						.map(ReactorServerAdapter::toReactorSignal)
						.dematerialize().log("DEMAT").cast(Signal.class);
				Flux<DataBuffer> fluxOfBuffers = riffSignalsAsFlux.map(this::convertToDataBuffer);

				Flux<?> objects = fluxOfBuffers
						.transform(this.decode(marshalling))
						.transform(fn)
						.transform(this.encode(marshalling));

				return objects.log("BEFORE");

			}
		}).log("FLATMAP")
				.materialize().log("MAT")
				.map(ReactorServerAdapter::toRiffSignal);
	}

	private Function<Flux<DataBuffer>, Flux<Object>> decode(AtomicReference<Marshalling> marshalling) {

		return db -> {
			for (Decoder decoder : decoders) {
				if (decoder.canDecode(fnInputType, marshalling.get().contentType)) {
					return decoder.decode(db, fnInputType, marshalling.get().contentType, null);
				}
			}
			return Flux.error(new RuntimeException("Could not find suitable decoder for " + marshalling.get().contentType));
		};
	}

	private Function<Flux<Object>, Flux<DataBuffer>> encode(AtomicReference<Marshalling> marshalling) {
		return os -> {

			MediaType accept = marshalling.get().accept;
			for (Encoder encoder : encoders) {
				for (Object mimeTypeO : encoder.getEncodableMimeTypes()) {
					MimeType mimeType = (MimeType) mimeTypeO;
					if (accept.includes(mimeType) && encoder.canEncode(fnOutputType, mimeType)) {
						return encoder.encode(os, dbf, fnOutputType, mimeType, null);
					}
				}
			}
			return Flux.error(new RuntimeException("Could not find an encoder accepted by " + accept));
		};
	}

	private DataBuffer convertToDataBuffer(Signal s) {
		return dbf.wrap(s.getNext().getPayload().asReadOnlyByteBuffer());
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

		final MediaType accept;

		private Marshalling(MimeType contentType, MediaType accept) {
			this.contentType = contentType;
			this.accept = accept;
		}
	}

}
