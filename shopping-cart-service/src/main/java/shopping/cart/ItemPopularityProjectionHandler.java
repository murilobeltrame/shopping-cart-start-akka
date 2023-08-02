package shopping.cart;

import akka.persistence.query.typed.EventEnvelope;
import akka.projection.jdbc.javadsl.JdbcHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.repository.HibernateJdbcSession;
import shopping.cart.repository.ItemPopularityRepository;

public final class ItemPopularityProjectionHandler extends JdbcHandler<EventEnvelope<ShoppingCart.Event>, HibernateJdbcSession> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String tag;
    private final ItemPopularityRepository repository;

    public ItemPopularityProjectionHandler(String tag, ItemPopularityRepository repository) {
        this.tag = tag;
        this.repository = repository;
    }

    private ItemPopularity findOrNew(String itemId) {
        return repository.findById(itemId)
                .orElseGet(() -> new ItemPopularity(itemId, 0, 0));
    }

    @Override
    public void process(HibernateJdbcSession session, EventEnvelope<ShoppingCart.Event> envelope) throws Exception {
        var event = envelope.event();
        if (event instanceof ShoppingCart.ItemAdded) {
            var added = (ShoppingCart.ItemAdded)event;
            var existingItemPopularity = findOrNew(added.itemId);
            var updatedItemPopularity = existingItemPopularity.changeCount(added.quantity);
            repository.save(updatedItemPopularity);
            logger.info(
                    "ItemPopularityProjectionHandler ({}) item popularity for '{}' : [{}]",
                    this.tag,
                    added.itemId,
                    updatedItemPopularity.getCount()
            );
        }
    }
}
