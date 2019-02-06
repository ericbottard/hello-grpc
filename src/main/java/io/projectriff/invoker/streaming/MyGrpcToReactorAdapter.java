package io.projectriff.invoker.streaming;

import java.util.function.Function;

import io.grpc.stub.StreamObserver;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;

public abstract class MyGrpcToReactorAdapter extends RiffGrpc.RiffImplBase {

	private final Function<Flux<Signal>, Flux<Signal>> reactorFn;

	public MyGrpcToReactorAdapter(Function<Flux<Signal>, Flux<Signal>> reactorFn) {
		this.reactorFn = reactorFn;
	}

	public abstract Flux<Signal> fn(Flux<Signal> in) ;

	@Override
	public StreamObserver<Signal> invoke(StreamObserver<Signal> responseObserver) {
		UnicastProcessor<Signal> processor = UnicastProcessor.create();
		FluxSink<Signal> sink = processor.sink();

		processor
				.compose(this::fn)
				.subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);

		return new StreamObserver<Signal>() {

			@Override
			public void onNext(Signal value) {
				sink.next(value);
			}

			@Override
			public void onError(Throwable t) {
				sink.error(t);
			}

			@Override
			public void onCompleted() {
				sink.complete();
			}
		};
	}
}
