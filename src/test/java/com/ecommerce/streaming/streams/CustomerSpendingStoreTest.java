package com.ecommerce.streaming.streams;

import com.ecommerce.streaming.model.CustomerSpending;
import com.ecommerce.streaming.support.TopologyTestFixture;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.ecommerce.streaming.support.TopologyTestFixture.at;
import static com.ecommerce.streaming.support.TopologyTestFixture.order;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code customer-spending-store} state store — the one Interactive Queries read.
 *
 * <p>Its value type is {@link CustomerSpending}, not {@code Double}. That distinction has bitten
 * this codebase twice: once as a {@code Double} running total that re-introduced binary rounding
 * error on every record, and once as a query-side
 * {@code ReadOnlyKeyValueStore<String, Double>} declaration that compiled cleanly (generic
 * erasure puts the cast at the call site, not in the store) and threw
 * {@code ClassCastException} on the first record at runtime. Asserting the concrete type here is
 * the check the compiler cannot make.
 *
 * <p>The store is deliberately UNWINDOWED: it is the customer's lifetime baseline, which is what
 * the BASELINE_DEVIATION signal needs. Time-scoped views live in the velocity and session stores.
 */
class CustomerSpendingStoreTest {

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
    @DisplayName("The store holds correctly typed CustomerSpending values with exact money")
    void storeHoldsTypedAggregates() {
        fixture.sendOrder(order("O-1", "C-1", "Books", "100.00", at(10)));
        fixture.sendOrder(order("O-2", "C-2", "Food",   "33.33", at(15)));
        fixture.sendOrder(order("O-3", "C-1", "Books", "250.50", at(20)));

        KeyValueStore<String, CustomerSpending> store = fixture.customerSpendingStore();

        CustomerSpending c1 = store.get("C-1");
        assertThat(c1)
                .as("the store value type is CustomerSpending, never Double")
                .isInstanceOf(CustomerSpending.class);
        assertThat(c1.getCustomerId()).isEqualTo("C-1");
        assertThat(c1.getOrderCount()).isEqualTo(2);
        assertThat(c1.getTotalSpent()).isEqualByComparingTo(new BigDecimal("350.50"));
        assertThat(c1.getAvgOrderValue()).isEqualByComparingTo(new BigDecimal("175.25"));
        // EVENT time of the most recent order, not the wall clock and not the broker timestamp.
        assertThat(c1.getLastOrderTimestamp()).isEqualTo(at(20).toEpochMilli());

        CustomerSpending c2 = store.get("C-2");
        assertThat(c2.getOrderCount()).isEqualTo(1);
        assertThat(c2.getTotalSpent()).isEqualByComparingTo(new BigDecimal("33.33"));
        assertThat(c2.getAvgOrderValue()).isEqualByComparingTo(new BigDecimal("33.33"));

        // A customer who has never ordered is absent, not zero — the query layer turns that into
        // 200 + found:false rather than a 404.
        assertThat(store.get("C-UNKNOWN")).isNull();
    }

    @Test
    @DisplayName("An out-of-order (late) record still updates the lifetime total, and lastOrderTimestamp does not go backwards")
    void lateRecordUpdatesTotalButNotTheLastOrderTimestamp() {
        fixture.sendOrder(order("O-1", "C-1", "Books", "100.00", at(60)));
        // Arrives second, but its event time is EARLIER.
        fixture.sendOrder(order("O-2", "C-1", "Books", "50.00", at(10)));

        CustomerSpending c1 = fixture.customerSpendingStore().get("C-1");
        assertThat(c1.getOrderCount()).isEqualTo(2);
        assertThat(c1.getTotalSpent()).isEqualByComparingTo(new BigDecimal("150.00"));
        assertThat(c1.getLastOrderTimestamp())
                .as("lastOrderTimestamp is a max over event times, so a late record cannot rewind it")
                .isEqualTo(at(60).toEpochMilli());
    }

    @Test
    @DisplayName("A hundred awkward amounts accumulate exactly, with no drift")
    void lifetimeTotalStaysExactOverManyRecords() {
        // The unwindowed store has no window to age drift out of: an error here is permanent and
        // grows with every restart. 100 x 0.07 = 7.00, exactly.
        for (int i = 0; i < 100; i++) {
            fixture.sendOrder(order("O-" + i, "C-DRIFT", "Food", "0.07", at(i)));
        }

        CustomerSpending spending = fixture.customerSpendingStore().get("C-DRIFT");
        assertThat(spending.getOrderCount()).isEqualTo(100);
        assertThat(spending.getTotalSpent()).isEqualByComparingTo(new BigDecimal("7.00"));
        assertThat(spending.getTotalSpent().toPlainString()).isEqualTo("7.00");
        assertThat(spending.getAvgOrderValue()).isEqualByComparingTo(new BigDecimal("0.07"));
    }
}
