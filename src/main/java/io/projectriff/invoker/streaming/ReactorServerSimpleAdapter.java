package io.projectriff.invoker.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import reactor.core.publisher.Flux;

import org.springframework.cloud.function.context.catalog.FunctionInspector;
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
import org.springframework.util.MimeType;


/**
 * Like {@link ReactorServerAdapter} but without (de)materialization.
 */
public class ReactorServerSimpleAdapter extends ReactorRiffGrpc.RiffImplBase {

	private final Function fn;

	private final FunctionInspector functionInspector;

	private final ResolvableType fnInputType;

	private final ResolvableType fnOutputType;

	private DataBufferFactory dbf = new DefaultDataBufferFactory();

	private List<Encoder> encoders = new ArrayList<>();

	private List<Decoder> decoders = new ArrayList<>();

	public ReactorServerSimpleAdapter(Function fn, FunctionInspector fi) {
		this.fn = fn;
		this.functionInspector = fi;
		this.fnInputType = ResolvableType.forClass(fi.getInputType(fn));
		//this.fnInputType = ResolvableType.forClass(String.class);

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

		return request
				.windowWhile(riffSignal -> {
					if (riffSignal.hasStart()) {
						if (!marshalling.compareAndSet(null, new Marshalling(riffSignal))) {
							// we already have a START frame
							throw new RuntimeException("Several START frames");
						}
						// closes the first (empty) window after having interpreted the start frame
						// also drops the start frame
						// everything that comes after this should go into the next window
						return false;
					}
					// not a START frame, pass through to the currently open window
					return true;
				})
				.flatMap(w -> {
					if (marshalling.get() == null)
						throw new RuntimeException("Should have seen Start Signal by now");

					Flux<Next> riffSignalsAsFlux = w.map(s -> s.getNext());
					Flux<DataBuffer> fluxOfBuffers = riffSignalsAsFlux.map(this::convertToDataBuffer);

					Flux<DataBuffer> objects = fluxOfBuffers
							.flatMap(this.decode(marshalling))
							.doOnNext(System.err::println)
							.transform(fn)
							.flatMap(this.encode(marshalling));

					return objects.log("BEFORE")
							.map(this::convertFromDataBuffer);

				})
				;
	}

	private Signal convertFromDataBuffer(DataBuffer b) {
		try {
			return Signal.newBuilder().setNext(Next.newBuilder().setPayload(ByteString.readFrom(b.asInputStream(true)))).build();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private Function<DataBuffer, Flux<Object>> decode(AtomicReference<Marshalling> marshalling) {

		return db -> {
			for (Decoder decoder : decoders) {
				if (decoder.canDecode(fnInputType, marshalling.get().contentType)) {
					return decoder.decode(Flux.just(db), fnInputType, marshalling.get().contentType, null);
				}
			}
			return Flux.error(
					new RuntimeException("Could not find suitable decoder for " + marshalling.get().contentType));
		};
	}

	private Function<Object, Flux<DataBuffer>> encode(AtomicReference<Marshalling> marshalling) {
		return o -> {

			MediaType accept = marshalling.get().accept;
			for (Encoder encoder : encoders) {
				for (Object mimeTypeO : encoder.getEncodableMimeTypes()) {
					MimeType mimeType = (MimeType) mimeTypeO;
					if (accept.includes(mimeType) && encoder.canEncode(ResolvableType.forInstance(o), mimeType)) {
						return encoder.encode(Flux.just(o), dbf, fnOutputType, mimeType, null);
					}
				}
			}
			return Flux.error(new RuntimeException("Could not find an encoder accepted by " + accept));
		};
	}

	private DataBuffer convertToDataBuffer(Next s) {
		return dbf.wrap(s.getPayload().asReadOnlyByteBuffer());
	}

	private static class Marshalling {
		final MimeType contentType;

		final MediaType accept;

		private Marshalling(Signal signal) {
			this.contentType = MimeType.valueOf(signal.getStart().getContentType());
			this.accept = MediaType.valueOf(signal.getStart().getAccept());
		}
	}

}
