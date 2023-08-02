package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.grpc.javadsl.ServerReflection;
import akka.grpc.javadsl.ServiceHandler;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import shopping.cart.proto.ShoppingCartService;
import shopping.cart.proto.ShoppingCartServiceHandlerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletionStage;

public final class ShoppingCartServer {
    private ShoppingCartServer(){}

    static void start(String host, int port, ActorSystem<?> system, ShoppingCartService grpcService) {
        @SuppressWarnings("unchecked")
        var service = ServiceHandler.concatOrNotFound(
                ShoppingCartServiceHandlerFactory.create(grpcService, system),
                ServerReflection.create(Collections.singletonList(ShoppingCartService.description), system)
        );
        var bound = Http.get(system).newServerAt(host, port).bind(service::apply);
        bound.whenComplete((binding, ex) -> {
           if (binding != null) {
               binding.addToCoordinatedShutdown(Duration.ofSeconds(3), system);
               var address = binding.localAddress();
               system
                       .log()
                       .info(
                               "Shopping online at gRPC server {}:{}",
                               address.getHostString(),
                               address.getPort()
                       );
           } else {
               system.log().error("Failed to bind gRPC endpoint, terminating system", ex);
               system.terminate();
           }
        });
    }
}
