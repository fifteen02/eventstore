package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.InterfaceStateEntity;
import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.InterfaceStateRepository;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import com.fifteen02.eventstore.to.Index;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Execution(ExecutionMode.CONCURRENT)
class CatchUpPlannerServiceTest {

    @Mock
    private InterfaceStateRepository interfaceStateRepository;

    @Mock
    private PersisterStateRepository persisterStateRepository;

    @Mock
    private IndexService indexService;

    @Mock
    private MessageFetcher archiveFetcher;

    @Mock
    private MessageFetcher archiveFetcher2;

    @Mock
    private MessageFetcher realtimeFetcher;

    private CatchUpPlannerService service;

    @BeforeEach
    void setUp() {
        lenient().when(archiveFetcher.name()).thenReturn("archive-1");
        lenient().when(archiveFetcher2.name()).thenReturn("archive-2");
        lenient().when(realtimeFetcher.name()).thenReturn("realtime");
        // Use a large maxSegmentSize in tests so existing expectations about
        // segment ranges remain valid unless the test is explicitly about batching.
        service = new CatchUpPlannerService(
                interfaceStateRepository,
                persisterStateRepository,
                indexService,
                List.of(archiveFetcher, archiveFetcher2, realtimeFetcher),
                1_000
        );
    }

    @Test
    void plan_returnsEmptyWhenRealtimeIndexIsNull() {
        var interfaceState = Fixtures.interfaceState("interface-a", 10L);
        when(interfaceStateRepository.findById("interface-a")).thenReturn(Optional.of(interfaceState));
        when(indexService.current()).thenReturn(new Index(null));

        var result = service.plan("interface-a");

        assertThat(result).isEmpty();
        verifyNoInteractions(persisterStateRepository);
    }

    @Test
    void plan_returnsEmptyWhenNoLag() {
        var interfaceState = Fixtures.interfaceState("interface-a", 10L);
        when(interfaceStateRepository.findById("interface-a")).thenReturn(Optional.of(interfaceState));
        when(indexService.current()).thenReturn(new Index(10L));

        var result = service.plan("interface-a");

        assertThat(result).isEmpty();
        verifyNoInteractions(persisterStateRepository);
    }

    @Test
    void plan_buildsSegmentsAcrossPersistorsInPriorityOrder() {
        var interfaceState = Fixtures.interfaceState("interface-a", 4L);
        when(interfaceStateRepository.findById("interface-a")).thenReturn(Optional.of(interfaceState));
        when(indexService.current()).thenReturn(new Index(20L));

        var archive1 = Fixtures.persistor("archive-1", 5L, 8L, 30, false);
        var archive2 = Fixtures.persistor("archive-2", 9L, 12L, 20, false);
        var realtime = Fixtures.persistor("realtime", 13L, 15L, 10, true);
        when(persisterStateRepository.findAllReadableOrderByPriorityDesc())
                .thenReturn(List.of(archive1, archive2, realtime));

        var archiveMessages = List.of(Fixtures.message(6L));
        var archive2Messages = List.of(Fixtures.message(11L));
        var realtimeMessages = List.of(Fixtures.message(19L));
        when(archiveFetcher.getMessages(5L, 8L)).thenReturn(archiveMessages);
        when(archiveFetcher2.getMessages(9L, 12L)).thenReturn(archive2Messages);
        when(realtimeFetcher.getMessages(13L, 20L)).thenReturn(realtimeMessages);

        var result = service.plan("interface-a");

        assertThat(result).isPresent();
        var plan = result.orElseThrow();
        assertThat(plan.lastAck()).isEqualTo(4L);
        assertThat(plan.target()).isEqualTo(20L);
        assertThat(plan.segments()).hasSize(3);
        assertThat(plan.segments().get(0).persistorId()).isEqualTo("archive-1");
        assertThat(plan.segments().get(0).fromInclusive()).isEqualTo(5L);
        assertThat(plan.segments().get(0).toInclusive()).isEqualTo(8L);
        assertThat(plan.segments().get(1).persistorId()).isEqualTo("archive-2");
        assertThat(plan.segments().get(1).fromInclusive()).isEqualTo(9L);
        assertThat(plan.segments().get(1).toInclusive()).isEqualTo(12L);
        assertThat(plan.segments().get(2).persistorId()).isEqualTo("realtime");
        assertThat(plan.segments().get(2).fromInclusive()).isEqualTo(13L);
        assertThat(plan.segments().get(2).toInclusive()).isEqualTo(20L);

        assertThat(plan.segments().get(0).messages().get()).isEqualTo(archiveMessages);
        assertThat(plan.segments().get(1).messages().get()).isEqualTo(archive2Messages);
        assertThat(plan.segments().get(2).messages().get()).isEqualTo(realtimeMessages);
    }

    @Test
    void planFromStartInclusive_buildsPlanWithoutInterfaceState() {
        when(indexService.current()).thenReturn(new Index(20L));

        var archive = Fixtures.persistor("archive-1", 5L, 12L, 30, false);
        var realtime = Fixtures.persistor("realtime", 13L, null, 10, true);
        when(persisterStateRepository.findAllReadableOrderByPriorityDesc())
                .thenReturn(List.of(archive, realtime));

        var result = service.planFromStartInclusive(10L);

        assertThat(result).isPresent();
        var plan = result.orElseThrow();
        assertThat(plan.lastAck()).isEqualTo(9L);
        assertThat(plan.target()).isEqualTo(20L);
        assertThat(plan.segments())
                .extracting(
                        CatchUpPlannerService.Segment::persistorId,
                        CatchUpPlannerService.Segment::fromInclusive,
                        CatchUpPlannerService.Segment::toInclusive)
                .containsExactly(
                        tuple("archive-1", 10L, 12L),
                        tuple("realtime", 13L, 20L));

        verifyNoInteractions(interfaceStateRepository);
    }

    @Test
    void planFromStartInclusiveToEndInclusive_clipsTargetToEndIndex() {
        when(indexService.current()).thenReturn(new Index(50L));

        var realtime = Fixtures.persistor("realtime", 1L, null, 10, true);
        when(persisterStateRepository.findAllReadableOrderByPriorityDesc())
                .thenReturn(List.of(realtime));

        var result = service.planFromStartInclusiveToEndInclusive(10L, 20L);

        assertThat(result).isPresent();
        var plan = result.orElseThrow();
        assertThat(plan.lastAck()).isEqualTo(9L);
        assertThat(plan.target()).isEqualTo(20L);
        assertThat(plan.segments()).singleElement().satisfies(segment -> {
            assertThat(segment.persistorId()).isEqualTo("realtime");
            assertThat(segment.fromInclusive()).isEqualTo(10L);
            assertThat(segment.toInclusive()).isEqualTo(20L);
        });

        verifyNoInteractions(interfaceStateRepository);
    }

    @Test
    void planFromStartInclusive_preservesMaxSegmentSize() {
        var smallSegmentService = new CatchUpPlannerService(
                interfaceStateRepository,
                persisterStateRepository,
                indexService,
                List.of(realtimeFetcher),
                3
        );
        when(indexService.current()).thenReturn(new Index(7L));

        var realtime = Fixtures.persistor("realtime", 1L, null, 10, true);
        when(persisterStateRepository.findAllReadableOrderByPriorityDesc())
                .thenReturn(List.of(realtime));

        var result = smallSegmentService.planFromStartInclusive(1L);

        assertThat(result).isPresent();
        assertThat(result.orElseThrow().segments())
                .extracting(
                        CatchUpPlannerService.Segment::fromInclusive,
                        CatchUpPlannerService.Segment::toInclusive)
                .containsExactly(
                        tuple(1L, 3L),
                        tuple(4L, 6L),
                        tuple(7L, 7L));
    }

    @Test
    void plan_createsSeparateSegmentsForSamePersistorWhenRangesAreDisjoint() {
        var interfaceState = Fixtures.interfaceState("interface-a", 4L);
        when(interfaceStateRepository.findById("interface-a")).thenReturn(Optional.of(interfaceState));
        when(indexService.current()).thenReturn(new Index(10L));

        var archivePart1 = Fixtures.persistor("archive-1", 5L, 7L, 30, false);
        var archivePart2 = Fixtures.persistor("archive-1", 8L, 10L, 20, false);
        when(persisterStateRepository.findAllReadableOrderByPriorityDesc())
                .thenReturn(List.of(archivePart1, archivePart2));

        var archivePart1Messages = List.of(Fixtures.message(6L));
        var archivePart2Messages = List.of(Fixtures.message(9L));
        when(archiveFetcher.getMessages(5L, 7L)).thenReturn(archivePart1Messages);
        when(archiveFetcher.getMessages(8L, 10L)).thenReturn(archivePart2Messages);

        var result = service.plan("interface-a");

        assertThat(result).isPresent();
        var plan = result.orElseThrow();
        assertThat(plan.segments()).hasSize(2);

        var first = plan.segments().get(0);
        var second = plan.segments().get(1);

        assertThat(first.persistorId()).isEqualTo("archive-1");
        assertThat(first.fromInclusive()).isEqualTo(5L);
        assertThat(first.toInclusive()).isEqualTo(7L);
        assertThat(first.messages().get()).isEqualTo(archivePart1Messages);

        assertThat(second.persistorId()).isEqualTo("archive-1");
        assertThat(second.fromInclusive()).isEqualTo(8L);
        assertThat(second.toInclusive()).isEqualTo(10L);
        assertThat(second.messages().get()).isEqualTo(archivePart2Messages);
    }

    @Test
    void plan_throwsWhenNothingCoversTheLagRange() {
        var interfaceState = Fixtures.interfaceState("interface-a", 4L);
        when(interfaceStateRepository.findById("interface-a")).thenReturn(Optional.of(interfaceState));
        when(indexService.current()).thenReturn(new Index(6L));

        var archive1 = Fixtures.persistor("archive-1", 10L, 12L, 30, false);
        when(persisterStateRepository.findAllReadableOrderByPriorityDesc())
                .thenReturn(List.of(archive1));

        assertThatThrownBy(() -> service.plan("interface-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing coverage from 5 to 6");
    }

    @Test
    void plan_throwsWhenGapExistsBetweenPersistors() {
        var interfaceState = Fixtures.interfaceState("interface-a", 0L);
        when(interfaceStateRepository.findById("interface-a")).thenReturn(Optional.of(interfaceState));
        when(indexService.current()).thenReturn(new Index(30L));

        var archive1 = Fixtures.persistor("archive-1", 1L, 10L, 30, false);
        var archive2 = Fixtures.persistor("archive-2", 11L, 20L, 20, false);
        var realtime = Fixtures.persistor("realtime", 22L, 30L, 10, true);
        when(persisterStateRepository.findAllReadableOrderByPriorityDesc())
                .thenReturn(List.of(archive1, archive2, realtime));

        assertThatThrownBy(() -> service.plan("interface-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing coverage from 21 to 21");
    }

    @Test
    void plan_throwsWhenInterfaceStateMissing() {
        when(interfaceStateRepository.findById("interface-a")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.plan("interface-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing interface state");
    }

    private static final class Fixtures {

        private static InterfaceStateEntity interfaceState(String name, long lastAck) {
            var state = new InterfaceStateEntity();
            state.setName(name);
            state.setLastAcknowledgedId(lastAck);
            return state;
        }

        private static PersisterStateEntity persistor(String id, Long start, Long last, int priority, boolean realtime) {
            var entity = new PersisterStateEntity();
            entity.setId(id);
            entity.setStartIndex(start);
            entity.setLastIndex(last);
            entity.setPriority(priority);
            entity.setCanRead(true);
            entity.setCanWrite(false);
            entity.setRealtime(realtime);
            return entity;
        }

        private static Message message(long id) {
            var message = new Message();
            message.setId(id);
            return message;
        }
    }
}
