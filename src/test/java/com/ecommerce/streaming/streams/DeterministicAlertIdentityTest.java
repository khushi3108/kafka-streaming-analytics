package com.ecommerce.streaming.streams;

import com.ecommerce.streaming.model.Order;
import com.ecommerce.streaming.model.OrderAlert;
import com.ecommerce.streaming.serde.JsonSerde;
import com.ecommerce.streaming.support.TopologyTestFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.ecommerce.streaming.support.TopologyTestFixture.at;
import static com.ecommerce.streaming.support.TopologyTestFixture.order;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Alert identity must be a pure function of the input records.
 *
 * <p>Alerts used to be stamped with {@code UUID.randomUUID()} and
 * {@code System.currentTimeMillis()}, so a redelivered order produced a byte-different second
 * alert and no consumer could tell "the same incident, seen twice" from "two incidents". That is
 * the property exactly-once processing cannot cover on its own: manual replay, reprocessing from
 * an earlier offset and the non-transactional dead-letter path all redeliver records.
 *
 * <p>The second half of the story is the opposite failure. Making an aggregate alert's id depend
 * on the customer ALONE would be deterministic and useless: every burst that customer ever
 * triggers would collapse onto one id, and a consumer deduping on alertId would drop every burst
 * after the first. The window start is what makes two bursts two events.
 */
class DeterministicAlertIdentityTest {

    private static final JsonSerde<OrderAlert> ALERT_SERDE = new JsonSerde<>(OrderAlert.class);

    private TopologyTestFixture fixture;

    @BeforeEach
    void setUp() {
        fixture = new TopologyTestFixture();
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    @DisplayName("Replaying the same order produces a BYTE-IDENTICAL alert")
    void replayingAnOrderProducesAByteIdenticalAlert() {
        Order theOrder = order("O-REPLAY", "C-1", "Electronics", "1500.00", at(10));

        // Same order, delivered twice with DIFFERENT broker timestamps — a realistic replay.
        // Neither the id nor the alert timestamp may move as a result.
        fixture.sendOrder(theOrder, at(10));
        fixture.sendOrder(theOrder, at(9 * 60));

        List<OrderAlert> alerts = fixture.readAlerts("HIGH_VALUE");
        assertThat(alerts).hasSize(2);

        OrderAlert first = alerts.get(0);
        OrderAlert second = alerts.get(1);

        assertThat(second.getAlertId()).isEqualTo(first.getAlertId());
        assertThat(second.getTimestamp()).isEqualTo(first.getTimestamp());
        // Event time of the order, not the (different) broker timestamps and not the wall clock.
        assertThat(first.getTimestamp()).isEqualTo(at(10).toEpochMilli());

        // The strongest form of the claim: identical on the wire, field for field.
        assertThat(ALERT_SERDE.serializer().serialize("fraud-alerts", second))
                .isEqualTo(ALERT_SERDE.serializer().serialize("fraud-alerts", first));
    }

    @Test
    @DisplayName("The same order under a DIFFERENT signal gets a different alert id")
    void differentSignalsOnTheSameOrderGetDifferentIds() {
        // Build enough history that the 6th order trips BASELINE_DEVIATION, and make that order
        // large enough to trip HIGH_VALUE as well: one order, two signals, two distinct alerts.
        for (int i = 0; i < 5; i++) {
            fixture.sendOrder(order("O-" + i, "C-B", "Books", "100.00", at(i * 5L)));
        }
        fixture.sendOrder(order("O-both", "C-B", "Books", "600.00", at(50)));

        List<OrderAlert> forThatOrder = fixture.readAlerts().stream()
                .filter(a -> "O-both".equals(a.getOrderId()))
                .toList();

        assertThat(forThatOrder).hasSize(2);
        assertThat(forThatOrder).extracting(OrderAlert::getAlertType)
                .containsExactlyInAnyOrder("HIGH_VALUE", "BASELINE_DEVIATION");
        assertThat(forThatOrder.get(0).getAlertId())
                .as("alertType is part of the identity, so two signals never share an id")
                .isNotEqualTo(forThatOrder.get(1).getAlertId());
    }

    @Test
    @DisplayName("Window-scoped aggregate alerts get a DIFFERENT id per window, not one id per customer")
    void aggregateAlertsAreScopedToTheWindowNotJustTheCustomer() {
        // Burst 1 — inside the first minute. Trailing 5-minute window starts at BASE-4m.
        fixture.sendOrder(order("O-1", "C-V", "Books", "100.00", at(0)));
        fixture.sendOrder(order("O-2", "C-V", "Books", "100.00", at(10)));
        fixture.sendOrder(order("O-3", "C-V", "Books", "100.00", at(20)));
        fixture.sendOrder(order("O-4", "C-V", "Books", "100.00", at(30)));

        // Burst 2 — six minutes later, by the SAME customer. Trailing window starts at BASE+2m,
        // and contains none of burst 1's orders.
        fixture.sendOrder(order("O-5", "C-V", "Books", "100.00", at(360)));
        fixture.sendOrder(order("O-6", "C-V", "Books", "100.00", at(370)));
        fixture.sendOrder(order("O-7", "C-V", "Books", "100.00", at(380)));
        fixture.sendOrder(order("O-8", "C-V", "Books", "100.00", at(390)));

        List<OrderAlert> velocity = fixture.readAlerts("VELOCITY");
        assertThat(velocity)
                .as("two separate bursts, one alert each")
                .hasSize(2);

        assertThat(velocity).extracting(OrderAlert::getCustomerId).containsExactly("C-V", "C-V");
        assertThat(velocity.get(0).getAlertId())
                .as("keying the id on customerId alone would collapse every burst onto one id, "
                        + "and a consumer deduping on alertId would drop every burst after the first")
                .isNotEqualTo(velocity.get(1).getAlertId());

        // Event time of each burst's last order — distinct, and derived from the records.
        assertThat(velocity.get(0).getTimestamp()).isEqualTo(at(30).toEpochMilli());
        assertThat(velocity.get(1).getTimestamp()).isEqualTo(at(390).toEpochMilli());
    }

    @Test
    @DisplayName("deterministicAlertId is a pure function — same inputs, same id, no clock, no randomness")
    void alertIdIsAPureFunctionOfItsInputs() {
        String a = OrderAlert.deterministicAlertId("O-1", "HIGH_VALUE", "MEDIUM");
        String b = OrderAlert.deterministicAlertId("O-1", "HIGH_VALUE", "MEDIUM");
        assertThat(a).isEqualTo(b);

        // Every component of the identity changes the id.
        assertThat(a).isNotEqualTo(OrderAlert.deterministicAlertId("O-2", "HIGH_VALUE", "MEDIUM"));
        assertThat(a).isNotEqualTo(OrderAlert.deterministicAlertId("O-1", "VELOCITY", "MEDIUM"));
        assertThat(a).isNotEqualTo(OrderAlert.deterministicAlertId("O-1", "HIGH_VALUE", "HIGH"));

        // A stable, pinned value: if the id derivation ever changes, every previously emitted
        // alertId changes with it and downstream dedupe stores silently stop matching. That is a
        // breaking change and this assertion is here to make it a deliberate one.
        assertThat(a).isEqualTo("b8d6ab60-cb7b-3142-a6e6-499239428ce4");
    }
}
