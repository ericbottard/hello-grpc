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
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        asyncStub = HelloGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void start() {
        StreamObserver<Reply> chelseaHandler = new StreamObserver<Reply>() {
            @Override
            public void onNext(Reply value) {
                System.out.println(String.format("%s a %d ans", value.getName(), value.getAge()));
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };

        StreamObserver<Person> ages1 = asyncStub.getAges(chelseaHandler);
        StreamObserver<Person> ages2 = asyncStub.getAges(chelseaHandler);

        ages1.onNext(Person.newBuilder().setBirthDate("1986-04-03").setName("Florent").build());
        ages2.onNext(Person.newBuilder().setBirthDate("1978-10-14").setName("Eric").build());
        ages2.onNext(Person.newBuilder().setBirthDate("1984-06-05").setName("Dilek").build());
    }

    public static void main(String[] args) throws InterruptedException {
        new LeClient("localhost", 9999).start();
        Thread.sleep(5000);
    }
}
