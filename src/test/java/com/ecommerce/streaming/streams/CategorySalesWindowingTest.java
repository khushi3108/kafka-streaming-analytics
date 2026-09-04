package com.ecommerce.streaming.streams;

import com.ecommerce.streaming.model.CategorySales;
import com.ecommerce.streaming.support.TopologyTestFixture;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static com.ecommerce.streaming.support.TopologyTestFixture.at;
import static com.ecommerce.streaming.support.TopologyTestFixture.order;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The category-sales branch: EXACT money, EVENT-time windowing, grace and suppression.
 *
 * <p>These four properties were each introduced by a separate commit and verified with a
 * throwaway driver that was then deleted. This class is the permanent version of those checks.
 *
 * <p>Money is compared with {@code compareTo} (via AssertJ's
 * {@code isEqualByComparingTo}) throughout: {@code new BigDecimal("0.30").equals(new
 * BigDecimal("0.3"))} is FALSE because equals compares scale as well as value, and a test that
 * used equals would fail for a reason that has nothing to do with correctness.
 */
class CategorySalesWindowingTest {

    private TopologyTestFixture fixture;

    @BeforeEach
    void setUp() {
        fixture = new TopologyTestFixture();
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    // ═══════════════════════════════════════════════════════════════════
    //  EXACT MONEY
    // ═══════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("BigDecimal aggregation is exact: 0.10 x 3 sums to exactly 0.30")
    void moneyAggregationIsExact() {
        // The failure this guards against, stated as an executable fact: the same sum in
        // binary floating point is 0.30000000000000004, and it drifts further with every record.
        double naiveDoubleSum = 0.10d + 0.10d + 0.10d;
        assertThat(naiveDoubleSum).isNotEqualTo(0.30d);

        fixture.sendOrder(order("O-1", "C-1", "Books", "0.10", at(10)));
        fixture.sendOrder(order("O-2", "C-2", "Books", "0.10", at(20)));
        fixture.sendOrder(order("O-3", "C-3", "Books", "0.10", at(30)));

        fixture.advanceStreamTimeTo(at(200));

        List<KeyValue<String, CategorySales>> emitted = fixture.readCategorySales();
        assertThat(emitted).hasSize(1);

        CategorySales sales = emitted.get(0).value;
        assertThat(sales.getOrderCount()).isEqualTo(3);
        assertThat(sales.getTotalSales()).isEqualByComparingTo(new BigDecimal("0.30"));
        // Not just numerically equal — the rendered value a consumer sees is "0.30" exactly.
        assertThat(sales.getTotalSales().toPlainString()).isEqualTo("0.30");
        assertThat(sales.getAvgOrderValue()).isEqualByComparingTo(new BigDecimal("0.10"));
        assertThat(sales.getMinOrderValue()).isEqualByComparingTo(new BigDecimal("0.10"));
        assertThat(sales.getMaxOrderValue()).isEqualByComparingTo(new BigDecimal("0.10"));
    }

    @Test
    @DisplayName("A long run of awkward cents stays exact through the windowed SUM")
    void manyAwkwardAmountsStayExact() {
        // 100 orders of 0.07. Exact answer: 7.00. In double arithmetic the running total is
        // 7.000000000000005 — the drift the BigDecimal migration existed to remove.
        for (int i = 0; i < 100; i++) {
            fixture.sendOrder(order("O-" + i, "C-" + i, "Food", "0.07", at(1 + (i % 50))));
        }
        fixture.advanceStreamTimeTo(at(200));

        List<KeyValue<String, CategorySales>> emitted = fixture.readCategorySales();
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).value.getOrderCount()).isEqualTo(100);
        assertThat(emitted.get(0).value.getTotalSales()).isEqualByComparingTo(new BigDecimal("7.00"));
        assertThat(emitted.get(0).value.getTotalSales().toPlainString()).isEqualTo("7.00");
    }

    // ═══════════════════════════════════════════════════════════════════
    //  EVENT TIME — the whole purpose of OrderTimestampExtractor
    // ═══════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Records with broker timestamps minutes apart land in ONE window when their payload timestamps share a minute")
    void windowsOnPayloadEventTimeNotBrokerTimestamp() {
        // Both orders carry a payload `timestamp` inside the first minute, but their RECORD
        // (broker append) timestamps are seven minutes apart. With the default
        // FailOnInvalidTimestamp extractor these would land in two different 1-minute windows and
        // the Java aggregates would disagree with ksqlDB, which windows on the payload field.
        fixture.sendOrder(order("O-1", "C-1", "Electronics", "10.00", at(30)),
                /* record timestamp */ at(0));
        fixture.sendOrder(order("O-2", "C-2", "Electronics", "20.00", at(45)),
                /* record timestamp */ at(7 * 60));

        fixture.advanceStreamTimeTo(at(300));

        List<KeyValue<String, CategorySales>> emitted = fixture.readCategorySales();

        // ONE window, not two.
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).key).isEqualTo("Electronics");
        assertThat(emitted.get(0).value.getOrderCount()).isEqualTo(2);
        assertThat(emitted.get(0).value.getTotalSales()).isEqualByComparingTo(new BigDecimal("30.00"));
    }

    @Test
    @DisplayName("Payload timestamps in different minutes still produce separate windows")
    void differentEventMinutesProduceSeparateWindows() {
        // The mirror image of the test above: proving the extractor is used, not that windowing
        // has been disabled. Same broker timestamp, payload timestamps a minute apart.
        fixture.sendOrder(order("O-1", "C-1", "Sports", "10.00", at(30)), at(30));
        fixture.sendOrder(order("O-2", "C-2", "Sports", "20.00", at(90)), at(30));

        fixture.advanceStreamTimeTo(at(400));

        List<KeyValue<String, CategorySales>> emitted = fixture.readCategorySales();
        assertThat(emitted).hasSize(2);
        assertThat(emitted).extracting(kv -> kv.value.getOrderCount()).containsExactly(1L, 1L);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  SUPPRESSION — one final result per window
    // ═══════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("A window emits exactly ONE final result, not one per input record")
    void windowEmitsExactlyOneFinalResult() {
        for (int i = 1; i <= 5; i++) {
            fixture.sendOrder(order("O-" + i, "C-" + i, "Clothing", "100.00", at(i * 5)));
        }

        // Before the window closes nothing at all has been emitted — suppress() is holding it.
        assertThat(fixture.readCategorySales()).isEmpty();

        fixture.advanceStreamTimeTo(at(200));

        List<KeyValue<String, CategorySales>> emitted = fixture.readCategorySales();
        assertThat(emitted)
                .as("suppress(untilWindowCloses) must collapse 5 updates into 1 final record")
                .hasSize(1);
        assertThat(emitted.get(0).value.getOrderCount()).isEqualTo(5);
        assertThat(emitted.get(0).value.getTotalSales()).isEqualByComparingTo(new BigDecimal("500.00"));
    }

    // ═══════════════════════════════════════════════════════════════════
    //  GRACE — 30 seconds, and not a millisecond more
    // ═══════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("A late record INSIDE the 30s grace period is still counted")
    void lateRecordWithinGraceIsCounted() {
        fixture.sendOrder(order("O-1", "C-1", "Books", "10.00", at(10)));
        fixture.sendOrder(order("O-2", "C-2", "Books", "20.00", at(20)));

        // Stream time = 75s. Window [0s,60s) expires at 60s + 30s grace = 90s, so it is still open.
        fixture.advanceStreamTimeTo(at(75));

        // Event time 30s: 45 seconds "late" in stream time, but well inside the grace period.
        fixture.sendOrder(order("O-3", "C-3", "Books", "30.00", at(30)));

        fixture.advanceStreamTimeTo(at(300));

        List<KeyValue<String, CategorySales>> emitted = fixture.readCategorySales();
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).value.getOrderCount())
                .as("the late-but-in-grace order must be part of the window's final answer")
                .isEqualTo(3);
        assertThat(emitted.get(0).value.getTotalSales()).isEqualByComparingTo(new BigDecimal("60.00"));
    }

    @Test
    @DisplayName("A late record PAST the 30s grace period is dropped from the aggregate")
    void lateRecordPastGraceIsDropped() {
        fixture.sendOrder(order("O-1", "C-1", "Books", "10.00", at(10)));
        fixture.sendOrder(order("O-2", "C-2", "Books", "20.00", at(20)));

        // Stream time = 200s, well past the window's 90s expiry. The window closes and emits.
        fixture.advanceStreamTimeTo(at(200));

        // Event time 30s — belongs to a window that has already expired.
        fixture.sendOrder(order("O-3", "C-3", "Books", "30.00", at(30)));
        fixture.advanceStreamTimeTo(at(400));

        List<KeyValue<String, CategorySales>> emitted = fixture.readCategorySales();
        assertThat(emitted)
                .as("the expired window must not be re-emitted with the too-late record folded in")
                .hasSize(1);
        assertThat(emitted.get(0).value.getOrderCount()).isEqualTo(2);
        assertThat(emitted.get(0).value.getTotalSales()).isEqualByComparingTo(new BigDecimal("30.00"));

        // Documented, deliberate asymmetry: the too-late order is dropped from the WINDOWED
        // aggregate but still reaches enriched-orders and still counts towards the unwindowed
        // customer-spending store. Asserting it here so the behaviour cannot change silently.
        assertThat(fixture.readEnrichedOrders())
                .extracting(com.ecommerce.streaming.model.EnrichedOrder::getOrderId)
                .contains("O-3");
    }
}
