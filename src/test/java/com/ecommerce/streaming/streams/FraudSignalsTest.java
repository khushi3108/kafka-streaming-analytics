package com.ecommerce.streaming.streams;

import com.ecommerce.streaming.model.OrderAlert;
import com.ecommerce.streaming.support.TopologyTestFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static com.ecommerce.streaming.support.TopologyTestFixture.at;
import static com.ecommerce.streaming.support.TopologyTestFixture.order;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The four anomaly signals, each exercised in isolation.
 *
 * <p>All four write to the same {@code fraud-alerts} topic and are discriminated by
 * {@code alertType}, so every test filters by type and — where it matters — also asserts the
 * TOTAL alert count, because "the right alert fired" and "only the right alert fired" are
 * different claims and only the second one is worth having.
 */
class FraudSignalsTest {

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
    //  SIGNAL 1 — HIGH_VALUE
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("SIGNAL 1: HIGH_VALUE")
    class HighValue {

        @Test
        @DisplayName("An order above the threshold raises exactly one HIGH_VALUE alert")
        void singleOrderOverThresholdAlerts() {
            fixture.sendOrder(order("O-1", "C-1", "Electronics", "750.00", at(10)));

            List<OrderAlert> alerts = fixture.readAlerts();
            assertThat(alerts).hasSize(1);

            OrderAlert alert = alerts.get(0);
            assertThat(alert.getAlertType()).isEqualTo("HIGH_VALUE");
            assertThat(alert.getOrderId()).isEqualTo("O-1");
            assertThat(alert.getCustomerId()).isEqualTo("C-1");
            assertThat(alert.getAmount()).isEqualByComparingTo(new BigDecimal("750.00"));
            // 750 is over 500 but not over 1000 → MEDIUM.
            assertThat(alert.getSeverity()).isEqualTo("MEDIUM");
            assertThat(alert.getTimestamp()).isEqualTo(at(10).toEpochMilli());
        }

        @Test
        @DisplayName("An order exactly AT the threshold does not alert — the comparison is strict")
        void orderExactlyAtThresholdDoesNotAlert() {
            fixture.sendOrder(order("O-1", "C-1", "Electronics", "500.00", at(10)));
            fixture.sendOrder(order("O-2", "C-2", "Electronics", "500.01", at(20)));

            List<OrderAlert> alerts = fixture.readAlerts("HIGH_VALUE");
            assertThat(alerts).hasSize(1);
            assertThat(alerts.get(0).getOrderId()).isEqualTo("O-2");
        }

        @Test
        @DisplayName("Severity is derived from the amount: MEDIUM / HIGH / CRITICAL")
        void severityLadder() {
            fixture.sendOrder(order("O-med",  "C-1", "Electronics", "600.00",  at(10)));
            fixture.sendOrder(order("O-high", "C-2", "Electronics", "1500.00", at(20)));
            fixture.sendOrder(order("O-crit", "C-3", "Electronics", "2500.00", at(30)));

            assertThat(fixture.readAlerts("HIGH_VALUE"))
                    .extracting(OrderAlert::getSeverity)
                    .containsExactly("MEDIUM", "HIGH", "CRITICAL");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  SIGNAL 2 — VELOCITY (hopping window)
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("SIGNAL 2: VELOCITY")
    class Velocity {

        @Test
        @DisplayName("More than 3 orders in 5 minutes alerts ONCE — not once per overlapping window")
        void burstAlertsOnceNotOncePerOverlappingWindow() {
            // A 5-minute window advancing every minute means every record belongs to FIVE windows
            // at the same time, and all five of them see all four orders. Without the
            // isTrailingWindow guard this burst would emit five near-identical alerts.
            fixture.sendOrder(order("O-1", "C-V", "Books", "100.00", at(0)));
            fixture.sendOrder(order("O-2", "C-V", "Books", "100.00", at(10)));
            fixture.sendOrder(order("O-3", "C-V", "Books", "100.00", at(20)));
            fixture.sendOrder(order("O-4", "C-V", "Books", "100.00", at(30)));

            List<OrderAlert> velocity = fixture.readAlerts("VELOCITY");
            assertThat(velocity)
                    .as("5 overlapping windows must still produce ONE alert for one burst")
                    .hasSize(1);

            OrderAlert alert = velocity.get(0);
            assertThat(alert.getCustomerId()).isEqualTo("C-V");
            assertThat(alert.getSeverity()).isEqualTo("HIGH");
            // Aggregate signals carry the window TOTAL, not any single order's amount...
            assertThat(alert.getAmount()).isEqualByComparingTo(new BigDecimal("400.00"));
            // ...and the order count — the fact that actually justifies the alert — in `reason`.
            assertThat(alert.getReason()).contains("4 orders");
            // Aggregate alerts are about a customer over a window, not about one order.
            assertThat(alert.getOrderId()).isNull();
            // EVENT time of the last order folded into the window, never the wall clock.
            assertThat(alert.getTimestamp()).isEqualTo(at(30).toEpochMilli());
        }

        @Test
        @DisplayName("Exactly 3 orders in the window do not alert — the threshold is strict")
        void threeOrdersDoNotAlert() {
            fixture.sendOrder(order("O-1", "C-V", "Books", "100.00", at(0)));
            fixture.sendOrder(order("O-2", "C-V", "Books", "100.00", at(10)));
            fixture.sendOrder(order("O-3", "C-V", "Books", "100.00", at(20)));

            assertThat(fixture.readAlerts()).isEmpty();
        }

        @Test
        @DisplayName("Orders spread beyond the 5-minute window do not accumulate into an alert")
        void ordersSpreadWiderThanTheWindowDoNotAlert() {
            // Four orders, but two minutes apart: no 5-minute window ever holds more than three.
            fixture.sendOrder(order("O-1", "C-V", "Books", "100.00", at(0)));
            fixture.sendOrder(order("O-2", "C-V", "Books", "100.00", at(120)));
            fixture.sendOrder(order("O-3", "C-V", "Books", "100.00", at(240)));
            fixture.sendOrder(order("O-4", "C-V", "Books", "100.00", at(360)));

            assertThat(fixture.readAlerts("VELOCITY")).isEmpty();
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  SIGNAL 3 — SESSION_BURST (session window)
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("SIGNAL 3: SESSION_BURST")
    class SessionBurst {

        @Test
        @DisplayName("A session of 8 orders raises ONE alert, and only after the session closes")
        void eightOrderSessionAlertsOnceAfterClose() {
            // 8 orders 30s apart: each gap is under the 5-minute inactivity gap, so this is one
            // session running from 0s to 210s.
            for (int i = 0; i < 8; i++) {
                fixture.sendOrder(order("O-" + i, "C-S", "Food", "100.00", at(i * 30L)));
            }

            // Nothing yet: suppress(untilWindowCloses) means a session's verdict is only
            // meaningful once the session has actually ended.
            assertThat(fixture.readAlerts("SESSION_BURST")).isEmpty();

            // Session end 210s + 5m inactivity gap + 1m grace = 570s. Move well past it.
            fixture.advanceStreamTimeTo(at(700));

            List<OrderAlert> session = fixture.readAlerts("SESSION_BURST");
            assertThat(session).hasSize(1);

            OrderAlert alert = session.get(0);
            assertThat(alert.getCustomerId()).isEqualTo("C-S");
            assertThat(alert.getAmount()).isEqualByComparingTo(new BigDecimal("800.00"));
            assertThat(alert.getReason()).contains("8 orders");
            // 800 is under the 3000 value trigger, so this fired on COUNT → HIGH, not CRITICAL.
            assertThat(alert.getSeverity()).isEqualTo("HIGH");
            assertThat(alert.getTimestamp()).isEqualTo(at(210).toEpochMilli());
        }

        @Test
        @DisplayName("A session over 3000 in total is CRITICAL however few orders it took")
        void highValueSessionIsCriticalOnValueAlone() {
            // Only 4 orders — under the 8-order trigger — but 3600 total, over the 3000 trigger.
            for (int i = 0; i < 4; i++) {
                fixture.sendOrder(order("O-" + i, "C-S", "Electronics", "900.00", at(i * 30L)));
            }
            fixture.advanceStreamTimeTo(at(700));

            List<OrderAlert> session = fixture.readAlerts("SESSION_BURST");
            assertThat(session).hasSize(1);
            assertThat(session.get(0).getSeverity()).isEqualTo("CRITICAL");
            assertThat(session.get(0).getAmount()).isEqualByComparingTo(new BigDecimal("3600.00"));
            assertThat(session.get(0).getReason()).contains("4 orders");
        }

        @Test
        @DisplayName("A quiet session (few orders, low value) closes without an alert")
        void ordinarySessionDoesNotAlert() {
            fixture.sendOrder(order("O-1", "C-S", "Food", "10.00", at(0)));
            fixture.sendOrder(order("O-2", "C-S", "Food", "10.00", at(30)));
            fixture.advanceStreamTimeTo(at(700));

            assertThat(fixture.readAlerts("SESSION_BURST")).isEmpty();
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  SIGNAL 4 — BASELINE_DEVIATION (stream ⋈ table)
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("SIGNAL 4: BASELINE_DEVIATION")
    class BaselineDeviation {

        @Test
        @DisplayName("A first-time customer is NOT flagged, however large the order")
        void firstTimeCustomerIsNeverFlagged() {
            // Without the min-prior-orders guard, a first order is trivially "3x the average of
            // nothing" and every new customer gets flagged on arrival.
            fixture.sendOrder(order("O-1", "C-NEW", "Electronics", "5000.00", at(10)));

            assertThat(fixture.readAlerts("BASELINE_DEVIATION")).isEmpty();
            // It IS a high-value order, so that signal fires — the two are independent.
            assertThat(fixture.readAlerts("HIGH_VALUE")).hasSize(1);
        }

        @Test
        @DisplayName("Fewer than 5 prior orders is not enough history to judge")
        void fewerThanFivePriorOrdersIsNotEnoughHistory() {
            // Four small orders then a big one: prior count is 4, one short of the minimum.
            for (int i = 0; i < 4; i++) {
                fixture.sendOrder(order("O-" + i, "C-B", "Books", "10.00", at(i * 5L)));
            }
            fixture.sendOrder(order("O-big", "C-B", "Books", "400.00", at(60)));

            assertThat(fixture.readAlerts("BASELINE_DEVIATION")).isEmpty();
        }

        @Test
        @DisplayName("An order over 3x the PRIOR average fires — and the order is excluded from its own baseline")
        void deviationFiresAndCurrentOrderIsExcludedFromItsOwnBaseline() {
            // Five prior orders of 100.00 → prior average 100.00, ceiling 300.00.
            for (int i = 0; i < 5; i++) {
                fixture.sendOrder(order("O-" + i, "C-B", "Books", "100.00", at(i * 5L)));
            }
            // 400.00 > 300.00 → alert.
            //
            // This amount is chosen precisely to prove the self-exclusion. The customer-spending
            // aggregate ALREADY contains this order by the time the join runs (the aggregate node
            // was added to the topology first), so a naive implementation would compare against
            // 900/6 = 150.00, giving a ceiling of 450.00 — and 400 < 450 would silently produce NO
            // alert. The alert existing at all is the assertion that the current order is backed
            // out of the baseline first.
            fixture.sendOrder(order("O-dev", "C-B", "Books", "400.00", at(50)));

            List<OrderAlert> alerts = fixture.readAlerts("BASELINE_DEVIATION");
            assertThat(alerts).hasSize(1);

            OrderAlert alert = alerts.get(0);
            assertThat(alert.getOrderId()).isEqualTo("O-dev");
            assertThat(alert.getCustomerId()).isEqualTo("C-B");
            assertThat(alert.getAmount()).isEqualByComparingTo(new BigDecimal("400.00"));
            assertThat(alert.getTimestamp()).isEqualTo(at(50).toEpochMilli());
            // 400 is below the 500 fraud threshold, so HIGH_VALUE deliberately did NOT fire:
            // this is the case a fixed threshold cannot see and a per-customer baseline can.
            assertThat(fixture.readAlerts("HIGH_VALUE")).isEmpty();
        }

        @Test
        @DisplayName("An order under 3x the prior average does not fire")
        void orderWithinTheBaselineDoesNotFire() {
            for (int i = 0; i < 5; i++) {
                fixture.sendOrder(order("O-" + i, "C-B", "Books", "100.00", at(i * 5L)));
            }
            // 250 < 3 x 100 = 300.
            fixture.sendOrder(order("O-ok", "C-B", "Books", "250.00", at(50)));

            assertThat(fixture.readAlerts("BASELINE_DEVIATION")).isEmpty();
        }
    }
}
