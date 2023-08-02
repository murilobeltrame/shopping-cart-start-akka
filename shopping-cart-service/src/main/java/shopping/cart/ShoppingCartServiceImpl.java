package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.grpc.GrpcServiceException;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.proto.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public final class ShoppingCartServiceImpl implements ShoppingCartService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Duration timeout;
    private final ClusterSharding sharding;

    public ShoppingCartServiceImpl(ActorSystem<?> system) {
        timeout = system.settings().config().getDuration("shopping-cart-service.ask-timeout");
        sharding = ClusterSharding.get(system);
    }

    @Override
    public CompletionStage<Cart> addItem(AddItemRequest in) {
        logger.info("addItem {} to cart {}", in.getItemId(), in.getCartId());

        var entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
        CompletionStage<ShoppingCart.Summary> reply = entityRef.askWithStatus(
                replyTo -> new ShoppingCart.AddItem(in.getItemId(), in.getQuantity(), replyTo),
                timeout
        );
        var cart = reply.thenApply(ShoppingCartServiceImpl::toProtoCart);
        return convertError(cart);
    }

    @Override
    public CompletionStage<Cart> checkout(CheckoutRequest in) {
        logger.info("checkout {}", in.getCartId());
        var entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
        var reply = entityRef.askWithStatus(ShoppingCart.Checkout::new, timeout);
        var cart = reply.thenApply(ShoppingCartServiceImpl::toProtoCart);
        return convertError(cart);
    }

    @Override
    public CompletionStage<Cart> getCart(GetCartRequest in) {
        logger.info("getCart {}", in.getCartId());
        var entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, in.getCartId());
        var reply = entityRef.ask(ShoppingCart.Get::new, timeout);
        var protoCart = reply.thenApply(cart -> {
            if (cart.items.isEmpty()) {
                throw new GrpcServiceException(Status.NOT_FOUND.withDescription("Cart "+in.getCartId()+" not found"));
            }
            return toProtoCart(cart);
        });
        return convertError(protoCart);
    }

    private static Cart toProtoCart(ShoppingCart.Summary cart) {
        var protoItems = cart
                .items
                .entrySet()
                .stream()
                .map(entry -> Item
                        .newBuilder()
                        .setItemId(entry.getKey())
                        .setQuantity(entry.getValue())
                        .build()
                )
                .collect(Collectors.toList());
        return Cart
                .newBuilder()
                .setCheckedOut(cart.checkedOut)
                .addAllItems(protoItems)
                .build();
    }

    private static <T> CompletionStage<T> convertError(CompletionStage<T> response) {
        return response
                .exceptionally(ex -> {
                   if (ex instanceof TimeoutException) {
                       throw new GrpcServiceException(
                               Status.UNAVAILABLE.withDescription("Operation timed out")
                       );
                   }
                   throw new GrpcServiceException(
                           Status.INVALID_ARGUMENT.withDescription(ex.getMessage())
                   );
                });
    }
}
