package net.biville.florent.riff;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class HelloServer {

    private final Server server;

    public HelloServer(int port) {
        server = ServerBuilder.forPort(port).addService(new HelloService()).build();
    }

    public void start() throws IOException {
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.print("Byyyyye");
            HelloServer.this.stop();
        }));
    }

    public void stop() {
        server.shutdown();
    }

    private void blockUntilShutdown() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        HelloServer server = new HelloServer(args.length > 0 ? Integer.parseInt(args[0], 10) : 9999);
        server.start();
        server.blockUntilShutdown();
    }
}
