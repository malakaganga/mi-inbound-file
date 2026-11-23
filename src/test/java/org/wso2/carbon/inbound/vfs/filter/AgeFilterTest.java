package org.wso2.carbon.inbound.vfs.filter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AgeFilterTest {
    @Test
    void zeroTimeUsesConfiguredAgeAcrossThreePolls() {
        List<String> warnings = new ArrayList<>();
        AgeFilter minimum = new AgeFilter(60L, null, warnings::add);
        AgeFilter maximum = new AgeFilter(null, 3600L, warnings::add);
        for (int poll = 0; poll < 3; poll++) {
            assertTrue(minimum.accept("minimum.csv", 0, 1000L * poll), "Age filter evidence: minimum");
            assertFalse(maximum.accept("maximum.csv", 0, 1000L * poll), "Age filter evidence: maximum");
        }
        assertEquals(List.of("The listing has no modified time for minimum.csv",
                "The listing has no modified time for maximum.csv"), warnings, "Age filter evidence: warnings");
        System.out.println("Observed zero time accepted by minimum age, skipped by maximum age on all three polls, one warning per file");
    }

    @Test
    void bothAgeLimitsUseStrictWholeSeconds() {
        AgeFilter filter = new AgeFilter(60L, 120L, message -> fail(message));
        long modified = 4000;
        assertFalse(filter.accept("data", modified, modified + 60999));
        assertTrue(filter.accept("data", modified, modified + 61000));
        assertTrue(filter.accept("data", modified, modified + 119999));
        assertFalse(filter.accept("data", modified, modified + 120000));
    }
}
