package net.biville.florent.riff;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import net.biville.florent.riff.hello.HelloGrpc;
import net.biville.florent.riff.hello.Person;
import net.biville.florent.riff.hello.Reply;

import java.util.concurrent.TimeUnit;

public class LeClient {

	private final ManagedChannel channel;

	private final HelloGrpc.HelloStub asyncStub;

	/**
	 * Construct client for accessing RouteGuide server at {@code host:port}.
	 */
	public LeClient(String host, int port) {
		channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().overrideAuthority(host)
                //.perRpcBufferLimit(4)
                //.maxInboundMessageSize(5)
                .build();
		asyncStub = HelloGrpc.newStub(channel);
	}

	public void start() throws InterruptedException {
		StreamObserver<Reply> chelseaHandler = new StreamObserver<Reply>() {
			@Override
			public void onNext(Reply value) {
				System.out.println(String.format("%s a %d ans", value.getName(), value.getAge()));
			}

			@Override
			public void onError(Throwable t) {
				System.err.println(t);
			}

			@Override
			public void onCompleted() {
				System.out.println("onComplete");
			}
		};


	    for (int i = 0 ; i < 80 ; i++) {
		    byte[] buf = new byte[i * 256];
	        StreamObserver<Person> stream = asyncStub.getAges(chelseaHandler);
		    stream.onNext(Person.newBuilder().setBirthDate("1986-04-03").setName("Flo" + buf.length + new String(buf)).build());
		    Thread.sleep(1000);
	    }

	}

	public static void main(String[] args) throws InterruptedException {
        new LeClient("grpc.default.example.com", 80).start();
        //new LeClient("localhost", 8080).start();
		Thread.sleep(50000);
	}
}
