package net.biville.florent.riff;

import io.grpc.stub.StreamObserver;
import net.biville.florent.riff.hello.HelloGrpc;
import net.biville.florent.riff.hello.Person;
import net.biville.florent.riff.hello.Reply;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class HelloService extends HelloGrpc.HelloImplBase {

    @Override
    public StreamObserver<Person> getAges(StreamObserver<Reply> responseObserver) {
        return new StreamObserver<Person>() {
            @Override
            public void onNext(Person person) {
                responseObserver.onNext(Reply.newBuilder()
                        .setAge(computeAge(person))
                        .setName(person.getName())
                        .build());

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    private long computeAge(Person person) {
        return ChronoUnit.YEARS.between(
                LocalDate.parse(person.getBirthDate(), DateTimeFormatter.ISO_LOCAL_DATE),
                LocalDate.now()
        );
    }
}
