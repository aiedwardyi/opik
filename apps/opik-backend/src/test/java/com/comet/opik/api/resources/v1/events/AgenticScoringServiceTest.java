package com.comet.opik.api.resources.v1.events;

import com.comet.opik.api.Span;
import com.comet.opik.api.Trace;
import com.comet.opik.api.resources.v1.events.tools.ToolRegistry;
import com.comet.opik.domain.SpanType;
import com.comet.opik.infrastructure.OnlineScoringConfig;
import com.comet.opik.utils.JsonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class AgenticScoringServiceTest {

    @Mock
    private OnlineScoringConfig onlineScoringConfig;

    private AgenticScoringService agenticScoringService;

    @BeforeEach
    void setUp() {
        agenticScoringService = new AgenticScoringServiceImpl(onlineScoringConfig, new ToolRegistry(Set.of()));
    }

    @Test
    @DisplayName("estimateThreadContextTokens reflects spans size, so big enriched threads route to agentic-tools")
    void estimateThreadContextTokensFactorsInSpansSize() {
        var traceId = UUID.randomUUID();
        var projectId = UUID.randomUUID();
        var trace = Trace.builder()
                .id(traceId)
                .projectId(projectId)
                .input(JsonUtils.getJsonNodeFromString("{\"q\":\"hi\"}"))
                .output(JsonUtils.getJsonNodeFromString("{\"a\":\"hello\"}"))
                .endTime(Instant.now())
                .build();
        var bigSpan = Span.builder()
                .id(UUID.randomUUID()).name("huge-tool-call").type(SpanType.tool)
                .startTime(Instant.now()).traceId(traceId).projectId(projectId)
                .input(JsonUtils.readTree("{\"payload\":\"" + "x".repeat(2000) + "\"}"))
                .build();

        int estimateNoSpans = agenticScoringService.estimateThreadContextTokens(
                List.of(trace), List.of(), 4);
        int estimateWithSpans = agenticScoringService.estimateThreadContextTokens(
                List.of(trace), List.of(bigSpan), 4);

        // Adding ~2KB of span payload must move the estimate up — otherwise the agentic-tools
        // routing gate would underestimate and inline-render an oversized prompt.
        assertThat(estimateWithSpans).isGreaterThan(estimateNoSpans);
    }

    @Test
    @DisplayName("preloadThreadSpansBounded keeps all spans and does not overflow when under the byte cap")
    void preloadUnderCapReturnsAllSpans() {
        var span1 = spanWithInput("a".repeat(100));
        var span2 = spanWithInput("b".repeat(100));

        var result = agenticScoringService
                .preloadThreadSpansBounded(Flux.just(span1, span2), 10_000L)
                .block();

        assertThat(result).isNotNull();
        assertThat(result.overflowed()).isFalse();
        assertThat(result.spans()).containsExactly(span1, span2);
    }

    @Test
    @DisplayName("preloadThreadSpansBounded overflows and drops the buffer when spans exceed the byte cap")
    void preloadOverCapOverflowsWithEmptyBuffer() {
        // Each span input is ~2 KB; a 1 KB cap is crossed by the first span.
        var big1 = spanWithInput("x".repeat(2000));
        var big2 = spanWithInput("y".repeat(2000));

        var result = agenticScoringService
                .preloadThreadSpansBounded(Flux.just(big1, big2), 1_000L)
                .block();

        assertThat(result).isNotNull();
        assertThat(result.overflowed()).isTrue();
        // Buffer dropped on overflow — the agentic-tools path re-fetches per-trace on demand.
        assertThat(result.spans()).isEmpty();
    }

    @Test
    @DisplayName("preloadThreadSpansBounded cancels the upstream once the cap is crossed (never drains the whole thread)")
    void preloadCancelsUpstreamOnOverflow() {
        var emitted = new AtomicInteger();
        var big = spanWithInput("x".repeat(2000));
        // Unbounded source: without early cancellation this would emit forever. The bounded preload
        // must stop (cancel) as soon as the running size crosses the cap — this is the OOM fix.
        Flux<Span> unbounded = Flux.<Span>generate(sink -> sink.next(big))
                .doOnNext(span -> emitted.incrementAndGet());

        var result = agenticScoringService.preloadThreadSpansBounded(unbounded, 1_000L).block();

        assertThat(result).isNotNull();
        assertThat(result.overflowed()).isTrue();
        assertThat(result.spans()).isEmpty();
        // Cancelled almost immediately — a handful of elements at most, not the unbounded stream.
        assertThat(emitted.get()).isLessThan(5);
    }

    private static Span spanWithInput(String payload) {
        return Span.builder()
                .id(UUID.randomUUID())
                .name("tool-call")
                .type(SpanType.tool)
                .startTime(Instant.now())
                .traceId(UUID.randomUUID())
                .projectId(UUID.randomUUID())
                .input(JsonUtils.readTree("{\"payload\":\"" + payload + "\"}"))
                .build();
    }
}
