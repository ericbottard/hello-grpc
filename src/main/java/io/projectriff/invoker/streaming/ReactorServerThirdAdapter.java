package io.projectriff.invoker.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import reactor.core.publisher.Flux;

import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.core.ResolvableType;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.ObjectToStringHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.MimeType;

/**
 * Like {@link ReactorServerAdapter} but without (de)materialization.
 */
public class ReactorServerThirdAdapter extends ReactorRiffGrpc.RiffImplBase {

	private final Function fn;

	private final ResolvableType fnInputType;

	private final ResolvableType fnOutputType;

	private List<HttpMessageConverter> converters = new ArrayList<>();

	public ReactorServerThirdAdapter(Function fn, FunctionInspector fi) {
		this.fn = fn;
		this.fnInputType = ResolvableType.forClass(fi.getInputType(fn));
		this.fnOutputType = ResolvableType.forClass(Integer.class);

		converters.add(new MappingJackson2HttpMessageConverter());
		converters.add(new FormHttpMessageConverter());
		StringHttpMessageConverter sc = new StringHttpMessageConverter();
		sc.setWriteAcceptCharset(false);
		converters.add(sc);
		ObjectToStringHttpMessageConverter oc = new ObjectToStringHttpMessageConverter(new DefaultConversionService());
		oc.setWriteAcceptCharset(false);
		converters.add(oc);
	}

	@Override
	public Flux<Signal> invoke(Flux<Signal> request) {
		AtomicReference<Marshalling> marshalling = new AtomicReference<>();

					//if (true) return Flux.error(new RuntimeException("test"));
		return request
				.windowWhile(riffSignal -> {
					if (riffSignal.hasStart()) {
						if (!marshalling.compareAndSet(null, new Marshalling(riffSignal))) {
							throw new RuntimeException("Several START frames");
						}
						return false;
					}
					return true;
				})
				.flatMap(w -> {
					if (marshalling.get() == null)
						throw new RuntimeException("Should have seen Start Signal by now");

					Flux<?> transform = w.map(toHttpMessage())
							.map(decode(marshalling))
							.transform(fn);
					return transform
							.map(encode(marshalling))
							.map(NextHttpOutputMessage::asSignal)
							.log("END");

				});
	}

	private Function<Object, NextHttpOutputMessage> encode(AtomicReference<Marshalling> marshalling) {
		return o -> {
			NextHttpOutputMessage out = new NextHttpOutputMessage();
			MediaType accept = marshalling.get().accept;
			for (HttpMessageConverter converter : converters) {
				for (Object mt : converter.getSupportedMediaTypes()) {
					MediaType mediaType = (MediaType) mt;
					if (o != null && accept.includes(mediaType) && converter.canWrite(o.getClass(), mediaType)) {
						try {
							converter.write(o, mediaType, out);
							return out;
						}
						catch (IOException e) {
							throw new HttpMessageNotWritableException("could not write message", e);
						}
					}
					else if (o == null  && accept.includes(mediaType) && converter.canWrite(fnOutputType.toClass(), mediaType)) {
						try {
							converter.write(o, mediaType, out);
							return out;
						}
						catch (IOException e) {
							throw new HttpMessageNotWritableException("could not write message", e);
						}
					}
				}
			}
			throw new HttpMessageNotWritableException(String.format("could not find converter for accept = '%s' and return value of type %s", accept, o == null ? "[null]" : o.getClass()));
		};
	}

	private Function<Signal, HttpInputMessage> toHttpMessage() {
		return s -> new HttpInputMessage() {
			@Override
			public InputStream getBody() throws IOException {
				return s.getNext().getPayload().newInput();
			}

			@Override
			public HttpHeaders getHeaders() {
				HttpHeaders headers = new HttpHeaders();
				s.getNext().getHeadersMap().entrySet()
						.forEach(e -> headers.add(e.getKey(), e.getValue()));
				return headers;
			}
		};
	}

	private Function<HttpInputMessage, Object> decode(AtomicReference<Marshalling> marshalling) {
		return m -> {
			MediaType contentType = m.getHeaders().getContentType();
			for (HttpMessageConverter converter : converters) {
				if (converter.canRead(fnInputType.toClass(), contentType)) {
					try {
						return converter.read(fnInputType.toClass(), m);
					}
					catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			throw new HttpMessageNotReadableException("No suitable converter", m);
		};
	}

	private static class Marshalling {
		final MimeType contentType;

		final MediaType accept;

		private Marshalling(Signal signal) {
			this.contentType = MimeType.valueOf(signal.getStart().getContentType());
			this.accept = MediaType.valueOf(signal.getStart().getAccept());
		}
	}

	private static class NextHttpOutputMessage implements HttpOutputMessage {

		private final ByteString.Output output = ByteString.newOutput();

		private final HttpHeaders headers = new HttpHeaders();

		@Override
		public OutputStream getBody() throws IOException {
			return output;
		}

		@Override
		public HttpHeaders getHeaders() {
			return headers;
		}

		public Signal asSignal() {
			return Signal.newBuilder().setNext(
					Next.newBuilder()
					.setPayload(output.toByteString())
					.putAllHeaders(headers.toSingleValueMap())
			).build();
		}

	}
}