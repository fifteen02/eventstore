package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.controller.StreamRequestParameters;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class OpenStreamsTest {

    private final OpenStreams openStreams = new OpenStreams();

    @Test
    void aNewStreamStartsJustBeforeItsStartIndex() {
        var stream = openStreams.open("a", parameters(42L), "curl/8.7.1", "127.0.0.1");

        assertThat(stream.position().get()).isEqualTo(41L);
        assertThat(openStreams.list()).containsExactly(stream);
    }

    @Test
    void positionOnlyMovesForward() {
        var stream = openStreams.open("a", parameters(1L), "browser", "127.0.0.1");

        stream.observed(10L);
        stream.observed(7L);

        assertThat(stream.position().get()).isEqualTo(10L);
    }

    @Test
    void closedStreamsAreNoLongerListed() {
        openStreams.open("a", parameters(1L), "browser", "127.0.0.1");
        var kept = openStreams.open("b", parameters(1L), "browser", "127.0.0.1");

        openStreams.close("a");

        assertThat(openStreams.list()).containsExactly(kept);
    }

    private static StreamRequestParameters parameters(Long startIndex) {
        return new StreamRequestParameters(Set.of(), Set.of(), startIndex, null, null, null);
    }
}
