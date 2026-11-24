/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.inbound.vfs;

import org.apache.axiom.om.OMOutputFormat;
import org.apache.axis2.AxisFault;
import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.provider.Fault;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.mockito.MockedStatic;
import org.wso2.vfs.shared.health.HealthRegistry;
import org.wso2.vfs.shared.failstate.FailState;
import org.wso2.vfs.shared.health.EndpointHealth;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;
import org.apache.axis2.transport.http.ApplicationXMLFormatter;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.inbound.InboundEndpointConstants;
import org.apache.synapse.inbound.InboundResponseSender;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VFSConsumerPollTest {
    @TempDir Path folder;
    final String endpoint = "poll" + UUID.randomUUID();
    final Properties properties = new Properties();
    final SynapseConfiguration configuration = mock(SynapseConfiguration.class);
    final SequenceMediator sequence = mock(SequenceMediator.class);
    final List<String> deliveries = Collections.synchronizedList(new ArrayList<>());
    final List<String> threads = Collections.synchronizedList(new ArrayList<>());
    final Map<Thread, VfsSession> owners = new java.util.concurrent.ConcurrentHashMap<>();
    interface SessionSetup {
        void apply(VfsSession session) throws FileSystemException;
    }
    SessionSetup setupSession = session -> { };
    boolean resetDuringDelivery;
    VFSConsumer consumer;

    java.util.function.Consumer<Axis2MessageContext> mediation = message -> { };

    SynapseEnvironment deploy(String uri, boolean sequential) throws Exception {
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(configuration.getSequence("process")).thenReturn(sequence);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn("poll");
        when(configuration.getInboundEndpoint(endpoint)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        axis.addMessageFormatter("application/xml", new ApplicationXMLFormatter());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenAnswer(call -> {
            Axis2MessageContext message = call.getArgument(0);
            Map<?, ?> headers = (Map<?, ?>) message.getAxis2MessageContext().getProperty(MessageContext.TRANSPORT_HEADERS);
            deliveries.add(headers.get("RELATIVE_PATH") + "/" + headers.get(VFSConstants.FILE_NAME));
            threads.add(Thread.currentThread().getName());
            if (resetDuringDelivery) {
                VfsSession owner = owners.get(Thread.currentThread());
                try (FileObject scratch = owner.resolve("ram:///ordinary-count", owner.options(VFSConstants.TRANSPORT_FILE_FILE_URI))) {
                    assertTrue(owner.reset(scratch.getFileSystem(), null));
                }
            }
            mediation.accept(message);
            return true;
        });
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, uri);
        properties.putIfAbsent("transport.vfs.ActionAfterProcess", "DELETE");
        properties.putIfAbsent("transport.vfs.ActionAfterFailure", "NONE");
        properties.setProperty("transport.vfs.ContentType", "application/xml");
        properties.setProperty("transport.vfs.Build", "true");
        consumer = new VFSConsumer(properties, endpoint, environment, 1000, "process", null, false, sequential) {
            @Override
            VfsSession newSession(long number) {
                VfsSession session = spy(super.newSession(number));
                doAnswer(call -> {
                    owners.put(call.getArgument(0), session);
                    return call.callRealMethod();
                }).when(session).enter(any(Thread.class));
                try {
                    setupSession.apply(session);
                } catch (FileSystemException problem) {
                    throw new AssertionError(problem);
                }
                return session;
            }
        };
        return environment;
    }

    @Test
    void firstPollWaitsForEndpointRegistrationBeforeConsumingSeed() throws Exception {
        Path seed = write("a.xml", "<a/>", 1);
        properties.setProperty("transport.vfs.ActionAfterFailure", "DELETE");
        SynapseEnvironment environment = deploy(folder.toUri().toString(), true);
        when(environment.getSynapseConfiguration()).thenReturn(null);
        consumer.poll();
        assertEquals("<a/>", Files.readString(seed));
        assertTrue(deliveries.isEmpty());
        assertEquals(0L, consumer.getHealth().get("filesDone"));
        assertEquals(0L, consumer.getHealth().get("filesFailed"));
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        InboundEndpoint registered = configuration.getInboundEndpoint(endpoint);
        when(configuration.getInboundEndpoint(endpoint)).thenReturn(null);
        consumer.poll();
        assertEquals("<a/>", Files.readString(seed));
        assertTrue(deliveries.isEmpty());
        when(configuration.getInboundEndpoint(endpoint)).thenReturn(registered);
        consumer.poll();
        consumer.poll();
        assertEquals(List.of("/a.xml"), deliveries);
        assertFalse(Files.exists(seed));
    }

    @Test
    void destroyedConsumerWarnsOnceAfterRemovalAndCannotTakeReplacementWork() throws Exception {
        deploy(folder.toUri().toString(), true);
        VFSConsumer old = consumer;
        old.destroy();
        when(configuration.getInboundEndpoint(endpoint)).thenReturn(null);
        Path seed = write("replacement.xml", "<replacement/>", 1);
        try (Logs logs = new Logs()) {
            old.poll();
            old.poll();
            assertEquals(1, logs.messages(Level.WARNING, endpoint, "polled while it is stopped").size());
            assertEquals("<replacement/>", Files.readString(seed));

            deploy(folder.toUri().toString(), true);
            old.poll();
            assertEquals("<replacement/>", Files.readString(seed));
            assertTrue(deliveries.isEmpty());
            consumer.poll();
            old.poll();
            consumer.poll();
            assertEquals(List.of("/replacement.xml"), deliveries);
            assertFalse(Files.exists(seed));
        }
    }

    @AfterEach
    void close() {
        if (consumer != null) {
            consumer.destroy();
            HealthRegistry.unregister(HealthRegistry.Kind.INBOUND, endpoint);
            consumer = null;
        }
    }

    Path write(String name, String body, int age) throws Exception {
        Path path = folder.resolve(name);
        Files.createDirectories(path.getParent());
        Files.writeString(path, body);
        Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis() - age * 1000L));
        return path;
    }

    @Test
    void directoryCombinesFiltersCountAndInterval() throws Exception {
        properties.setProperty("transport.vfs.FileNamePattern", ".*");
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.FileSortAscending", "true");
        properties.setProperty("transport.vfs.FileSizeLimit", "4");
        properties.setProperty("transport.vfs.MinimumAge", "60");
        properties.setProperty("transport.vfs.MaximumAge", "120");
        properties.setProperty("transport.vfs.FileProcessCount", "2");
        properties.setProperty("transport.vfs.FileProcessInterval", "40");
        write("00.xml", "<large/>", 90);
        write("01.xml", "<x/>", 10);
        write("02.xml", "<x/>", 150);
        write("a.xml", "<a/>", 90);
        write("b.xml", "<b/>", 90);
        write("c.xml", "<c/>", 90);
        write("reserved.lock", "<r/>", 90);
        deploy(folder.toUri().toString(), true);
        long start = System.nanoTime();
        consumer.poll();
        assertEquals(List.of("/a.xml", "/b.xml"), deliveries);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 80);
        consumer.poll();
        assertEquals(List.of("/a.xml", "/b.xml", "/c.xml"), deliveries);
        assertTrue(Files.exists(folder.resolve("reserved.lock")));
    }

    @Test
    void recursiveWorkersSkipTheTempFolderAndReportRelativePaths() throws Exception {
        properties.setProperty("transport.vfs.WorkerPoolSize", "2");
        properties.setProperty("transport.vfs.CheckSizeInterval", "10");
        write("sub/a.xml", "<a/>", 1);
        write("b.xml", "<b/>", 1);
        write(".part/internal.xml", "<t/>", 1);
        deploy(folder.toUri().toString() + "*", false);
        consumer.poll();
        assertEquals(2, deliveries.size());
        assertTrue(deliveries.containsAll(List.of("/sub/a.xml", "/b.xml")));
        assertTrue(threads.stream().allMatch(thread -> thread.startsWith("vfs-inbound-" + endpoint + "-worker-")));
        assertTrue(Files.exists(folder.resolve(".part/internal.xml")));
        assertFalse(Files.exists(folder.resolve("sub/a.xml")));
    }

    @Test
    void singleFileIsPolledAgainAfterAbsenceAndResume() throws Exception {
        Path file = folder.resolve("single.xml");
        deploy(file.toUri().toString(), false);
        consumer.poll();
        write("single.xml", "<a/>", 1);
        consumer.poll();
        assertFalse(Files.exists(file));
        consumer.pause();
        consumer.resume();
        write("single.xml", "<b/>", 1);
        consumer.poll();
        assertEquals(List.of("/single.xml", "/single.xml"), deliveries);
        assertFalse(Files.exists(file));
    }

    @Test
    void missingSequenceLeavesTheFileForTheNextPoll() throws Exception {
        Path file = write("one.xml", "<x/>", 1);
        deploy(folder.toUri().toString(), true);
        when(configuration.getSequence("process")).thenReturn(null);
        consumer.poll();
        assertTrue(Files.exists(file));
        assertTrue(deliveries.isEmpty());
        when(configuration.getSequence("process")).thenReturn(sequence);
        consumer.poll();
        assertEquals(List.of("/one.xml"), deliveries);
    }
    @Test
    void thirdPollSessionResetEndsAdmissionAndTheNextPollStartsFresh() throws Exception {
        resetDuringDelivery = true;
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.FileSortAscending", "true");
        for (int index = 0; index < 5; index++) {
            write("reset" + index + ".xml", "<r/>", 1);
        }
        deploy(folder.toUri().toString(), true);
        consumer.poll();
        assertEquals(List.of("/reset0.xml", "/reset1.xml", "/reset2.xml"), deliveries);
        assertFalse(Files.exists(folder.resolve("reset2.xml")));
        assertTrue(Files.exists(folder.resolve("reset3.xml")));
        consumer.poll();
        assertEquals(List.of("/reset0.xml", "/reset1.xml", "/reset2.xml", "/reset3.xml", "/reset4.xml"), deliveries);
    }

    @Test
    void workerSessionResetsDoNotSpendThePollSessionBudget() throws Exception {
        resetDuringDelivery = true;
        properties.setProperty("transport.vfs.WorkerPoolSize", "1");
        for (int index = 0; index < 5; index++) {
            write("worker" + index + ".xml", "<w/>", 1);
        }
        deploy(folder.toUri().toString(), false);
        consumer.poll();
        assertEquals(5, deliveries.size());
        for (int index = 0; index < 5; index++) {
            assertFalse(Files.exists(folder.resolve("worker" + index + ".xml")));
        }
    }

    @Test
    void aWorkerThatEndedWithAnErrorDoesNotStopThePoll() throws Exception {
        write("a.xml", "<a/>", 2);
        write("b.xml", "<b/>", 1);
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.WorkerPoolSize", "1");
        deploy(folder.toUri().toString(), false);
        mediation = message -> {
            Map<?, ?> headers = (Map<?, ?>) message.getAxis2MessageContext()
                    .getProperty(MessageContext.TRANSPORT_HEADERS);
            if ("a.xml".equals(headers.get(VFSConstants.FILE_NAME))) {
                throw new NoClassDefFoundError("Test worker error.");
            }
        };
        try (Logs logs = new Logs()) {
            consumer.poll();
            assertEquals(List.of("/a.xml", "/b.xml"), deliveries);
            assertTrue(Files.exists(folder.resolve("a.xml")));
            assertFalse(Files.exists(folder.resolve("b.xml")));
            assertTrue(logs.messages(Level.SEVERE, endpoint, "Poll failed").isEmpty());
            assertEquals(1, logs.messages(Level.SEVERE, endpoint, "A file worker failed").size());
        }
    }

    @Test
    void aCappedFileThatLeftTheListingLosesItsCountAndItsFlag() throws Exception {
        Path file = write("a.xml", "<a/>", 1);
        String uri = file.toUri().toString();
        properties.setProperty("transport.vfs.WorkerPoolSize", "1");
        properties.setProperty("transport.vfs.MaxReadTransportErrors", "1");
        String root = folder.toUri().toString();
        java.util.concurrent.atomic.AtomicBoolean metadataRefused = new java.util.concurrent.atomic.AtomicBoolean();
        setupSession = session -> {
            // Every read of the file content breaks the connection. The listing and the lock work.
            doAnswer(call -> {
                FileObject view = spy((FileObject) call.callRealMethod());
                FileSystemException lost = new FileSystemException("Test read connection lost.");
                lost.setFault(Fault.TRANSPORT);
                doThrow(lost).when(view).getContent();
                return view;
            }).when(session).resolve(eq(uri), any());
            // When asked, the listing shows the file but its metadata read fails with an application error.
            doAnswer(call -> {
                FileObject resolved = (FileObject) call.callRealMethod();
                if (!metadataRefused.get()) {
                    return resolved;
                }
                FileObject view = spy(resolved);
                doAnswer(list -> {
                    FileObject[] children = resolved.getChildren();
                    for (int index = 0; index < children.length; index++) {
                        if (children[index].getName().getBaseName().equals("a.xml")) {
                            FileObject child = spy(children[index]);
                            FileSystemException refused = new FileSystemException("Test metadata refused.");
                            refused.setFault(Fault.APPLICATION);
                            doThrow(refused).when(child).getContent();
                            children[index] = child;
                        }
                    }
                    return children;
                }).when(view).getChildren();
                return view;
            }).when(session).resolve(eq(root), any());
        };
        deploy(root, false);
        consumer.poll();
        assertEquals(0L, readCapFlags());
        consumer.poll();
        assertEquals(1L, readCapFlags());
        assertTrue(Files.exists(file));
        assertTrue(deliveries.isEmpty());
        // A file that is listed and skipped keeps its count.
        metadataRefused.set(true);
        consumer.poll();
        assertEquals(1L, readCapFlags());
        metadataRefused.set(false);
        Files.delete(file);
        consumer.poll();
        assertEquals(0L, readCapFlags());
    }

    private long readCapFlags() {
        Map<?, ?> flags = (Map<?, ?>) consumer.getHealth().get("flags");
        Map<?, ?> detail = flags == null ? null : (Map<?, ?>) flags.get("READ_CAP");
        return detail == null ? 0L : (Long) detail.get("count");
    }

    @Test
    void aMediationFailureAndAWorkerFailureAreLoggedWithTheirStackTrace() throws Exception {
        write("a.xml", "<a/>", 2);
        write("b.xml", "<b/>", 1);
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.WorkerPoolSize", "1");
        deploy(folder.toUri().toString(), false);
        mediation = message -> {
            Map<?, ?> headers = (Map<?, ?>) message.getAxis2MessageContext()
                    .getProperty(MessageContext.TRANSPORT_HEADERS);
            if ("a.xml".equals(headers.get(VFSConstants.FILE_NAME))) {
                throw new IllegalStateException();
            }
            throw new NoClassDefFoundError("Test worker error.");
        };
        try (Logs logs = new Logs()) {
            consumer.poll();
            List<String> mediated = logs.messages(Level.SEVERE, endpoint, "File mediation failed.");
            assertEquals(1, mediated.size());
            assertTrue(mediated.get(0).contains("java.lang.IllegalStateException"), mediated.get(0));
            assertTrue(mediated.get(0).contains("\tat "), mediated.get(0));
            List<String> worker = logs.messages(Level.SEVERE, endpoint, "A file worker failed.");
            assertEquals(1, worker.size());
            assertTrue(worker.get(0).contains("java.lang.NoClassDefFoundError: Test worker error."), worker.get(0));
            assertTrue(worker.get(0).contains("\tat "), worker.get(0));
        }
    }

    // A temp name of a cut copy, as the shared module builds it: the target name, the creation second and a random
    // part, each 7 base 36 digits, and the suffix.
    private static String tempName(String target, long createdSeconds) {
        String created = Long.toString(createdSeconds, 36);
        return target + "." + "0000000".substring(created.length()) + created + "abc1234.part";
    }

    // The folders of a move setup: a success folder and a failure folder, with the files a sweep must judge.
    private Path moveSetup(Path out, Path failed) throws Exception {
        Path input = Files.createDirectory(folder.resolve("in"));
        Files.createDirectory(out);
        Files.createDirectory(failed);
        properties.setProperty("transport.vfs.ActionAfterProcess", "MOVE");
        properties.setProperty("transport.vfs.MoveAfterProcess", out.toUri().toString());
        properties.setProperty("transport.vfs.ActionAfterFailure", "MOVE");
        properties.setProperty("transport.vfs.MoveAfterFailure", failed.toUri().toString());
        mediation = message -> {
            Map<String, Object> headers = (Map<String, Object>) message.getAxis2MessageContext()
                    .getProperty(MessageContext.TRANSPORT_HEADERS);
            if (headers.get(VFSConstants.FILE_NAME).toString().startsWith("bad")) {
                headers.put(VFSConstants.ERROR_CODE, "1");
            }
        };
        return input;
    }

    @Test
    void moveTargetSweepRemovesOnlyAStaleTempFileOfAFolderThatThePollUsed() throws Exception {
        Path out = folder.resolve("out");
        Path failed = folder.resolve("failed");
        Path input = moveSetup(out, failed);
        long now = System.currentTimeMillis() / 1000;
        long old = now - TimeUnit.DAYS.toSeconds(2);
        Path stale = Files.writeString(out.resolve(tempName("a.xml", old)), "abc");
        Path young = Files.writeString(out.resolve(tempName("b.xml", now)), "abc");
        Path growing = Files.writeString(out.resolve(tempName("c.xml", old)), "abc");
        Path plain = Files.writeString(out.resolve("notes.txt"), "abc");
        Path shape = Files.writeString(out.resolve("d.xml.0000000.part"), "abc");
        Path unused = Files.writeString(failed.resolve(tempName("e.xml", old)), "abc");
        AtomicLong elapsed = new AtomicLong();
        try (MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
            clocks.when(() -> TestPoints.nanoTime("cleanup")).thenAnswer(call ->
                    TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
            deploy(input.toUri().toString(), true);
            Files.writeString(input.resolve("good1.xml"), "<g/>");
            Files.writeString(input.resolve("bad1.xml"), "<b/>");
            consumer.poll();
            assertTrue(Files.isRegularFile(out.resolve("good1.xml")));
            assertTrue(Files.isRegularFile(failed.resolve("bad1.xml")));
            // The first sweep of each folder only notes the sizes.
            for (Path kept : Arrays.asList(stale, young, growing, plain, shape, unused)) {
                assertTrue(Files.exists(kept), kept.toString());
            }
            Files.writeString(growing, "abcdef");
            elapsed.set(TimeUnit.MINUTES.toMillis(61));
            Files.writeString(input.resolve("good2.xml"), "<g/>");
            consumer.poll();
            assertTrue(Files.isRegularFile(out.resolve("good2.xml")));
            assertFalse(Files.exists(stale), "A stale temp file with the same size an hour apart is removed");
            assertTrue(Files.exists(young), "A temp file younger than the stale age stays");
            assertTrue(Files.exists(growing), "A temp file whose size changed stays");
            assertTrue(Files.exists(plain), "A name that is not a temp name stays");
            assertTrue(Files.exists(shape), "A name with the wrong shape stays");
            assertTrue(Files.exists(unused), "A folder that this poll did not move into is not swept");
        } finally {
            close();
        }
    }

    @Test
    void moveTargetSweepWaitsWhileThePollSessionIsNotUsable() throws Exception {
        Path out = folder.resolve("out");
        Path input = moveSetup(out, folder.resolve("failed"));
        Path stale = Files.writeString(out.resolve(tempName("a.xml",
                System.currentTimeMillis() / 1000 - TimeUnit.DAYS.toSeconds(2))), "abc");
        AtomicLong elapsed = new AtomicLong();
        try (MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
            clocks.when(() -> TestPoints.nanoTime("cleanup")).thenAnswer(call ->
                    TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
            properties.setProperty("transport.vfs.FileSortAttribute", "Name");
            deploy(input.toUri().toString(), true);
            Files.writeString(input.resolve("first.xml"), "<f/>");
            consumer.poll();
            elapsed.set(TimeUnit.MINUTES.toMillis(61));
            // Three session resets in one poll end its admission, so its session is not used for the sweep.
            resetDuringDelivery = true;
            for (int index = 0; index < 3; index++) {
                Files.writeString(input.resolve("reset" + index + ".xml"), "<r/>");
            }
            consumer.poll();
            assertTrue(Files.isRegularFile(out.resolve("reset2.xml")));
            assertTrue(Files.exists(stale), "No sweep with a session that spent its resets");
            resetDuringDelivery = false;
            Files.writeString(input.resolve("last.xml"), "<l/>");
            consumer.poll();
            assertFalse(Files.exists(stale), "The next usable poll sweeps the folder");
        } finally {
            close();
        }
    }

    @Test
    void failedMoveTargetSweepLeavesThePollAndTheMovesAndWarnsOnce() throws Exception {
        Path out = folder.resolve("out");
        Path input = moveSetup(out, folder.resolve("failed"));
        setupSession = session -> doThrow(new IllegalStateException("Sweep fixture"))
                .when(session).lockIo(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS);
        AtomicLong elapsed = new AtomicLong();
        try (Logs logs = new Logs(); MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
            clocks.when(() -> TestPoints.nanoTime("cleanup")).thenAnswer(call ->
                    TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
            deploy(input.toUri().toString(), true);
            Files.writeString(input.resolve("one.xml"), "<o/>");
            consumer.poll();
            elapsed.set(TimeUnit.MINUTES.toMillis(61));
            Files.writeString(input.resolve("two.xml"), "<t/>");
            consumer.poll();
            assertEquals(List.of("/one.xml", "/two.xml"), deliveries);
            assertTrue(Files.isRegularFile(out.resolve("one.xml")));
            assertTrue(Files.isRegularFile(out.resolve("two.xml")));
            assertFalse(Files.exists(input.resolve("one.xml.fail")));
            assertFalse(Files.exists(input.resolve("two.xml.fail")));
            assertEquals(1, logs.messages(Level.WARNING, endpoint, "were not swept").size());
        } finally {
            close();
        }
    }

    private static final class Logs extends Handler implements AutoCloseable {
        final Logger root = Logger.getLogger("");
        final List<LogRecord> records = new ArrayList<>();

        Logs() {
            root.addHandler(this);
        }

        @Override public void publish(LogRecord record) { records.add(record); }
        @Override public void flush() { }
        @Override public void close() { root.removeHandler(this); }

        List<String> messages(Level level, String endpoint, String text) {
            List<String> matches = new ArrayList<>();
            for (LogRecord record : records) {
                String message = record.getMessage();
                if (record.getLevel().equals(level) && message.contains(endpoint) && message.contains(text)) {
                    matches.add(message);
                }
            }
            return matches;
        }
    }

    @Test
    void listingRuntimeReturnsNormallyAndErrorLeavesThePoll() throws Exception {
        for (boolean single : new boolean[]{false, true}) {
            Path file = write(single ? "single.xml" : "folder.xml", "<x/>", 1);
            String uri = (single ? file : folder).toUri().toString();
            AtomicReference<Throwable> next = new AtomicReference<>(new IllegalStateException("Listing fixture"));
            setupSession = session -> doAnswer(call -> {
                FileObject resolved = (FileObject) call.callRealMethod();
                Throwable thrown = next.getAndSet(null);
                if (thrown == null) {
                    return resolved;
                }
                FileObject view = spy(resolved);
                doAnswer(type -> { throw thrown; }).when(view).getType();
                return view;
            }).when(session).resolve(eq(uri), any());
            deploy(uri, true);
            try (Logs logs = new Logs()) {
                assertNull(consumer.poll());
                assertEquals(1, logs.messages(Level.SEVERE, endpoint, "Poll failed").size());
                assertTrue(logs.messages(Level.SEVERE, endpoint, "Poll failed").get(0)
                        .contains("java.lang.IllegalStateException: Listing fixture"));
                assertTrue(Files.exists(file));
                consumer.poll();
                assertFalse(Files.exists(file));
                next.set(new AssertionError("Listing error fixture"));
                assertThrows(AssertionError.class, consumer::poll);
            } finally {
                close();
            }
        }
    }

    // A listed name whose type read fails with a fault that is not transport, as the SMB2 share does for a name it
    // refuses. The other files of the folder are still delivered and the refused name stays untouched.
    @Test
    void refusedChildTypeSkipsOnlyThatFileAndTransportStillLeavesThePoll() throws Exception {
        Path first = write("a.xml", "<a/>", 1);
        Path refused = write("refused.xml", "<refused/>", 1);
        Path last = write("z.xml", "<z/>", 1);
        // A name that the server refuses has no file under that name, as on SMB2. Its fail marker, here in the
        // format of the old transport, must stay.
        Path marker = write("ghost.xml.fail", "1795000000000", 1);
        String uri = folder.toUri().toString();
        AtomicReference<Fault> fault = new AtomicReference<>(Fault.APPLICATION);
        setupSession = session -> doAnswer(call -> {
            FileObject resolved = (FileObject) call.callRealMethod();
            FileObject view = spy(resolved);
            doAnswer(list -> {
                FileObject[] children = resolved.getChildren();
                for (int index = 0; index < children.length; index++) {
                    if (children[index].getName().getBaseName().equals("refused.xml")) {
                        FileObject child = spy(children[index]);
                        FileSystemException problem = new FileSystemException("The type fixture refused the name.");
                        problem.setFault(fault.get());
                        doThrow(problem).when(child).getType();
                        children[index] = child;
                    }
                }
                FileObject ghost = spy(resolved.resolveFile("ghost.xml"));
                FileSystemException problem = new FileSystemException("The type fixture refused the name.");
                problem.setFault(fault.get());
                doThrow(problem).when(ghost).getType();
                FileObject[] listed = Arrays.copyOf(children, children.length + 1);
                listed[children.length] = ghost;
                return listed;
            }).when(view).getChildren();
            return view;
        }).when(session).resolve(eq(uri), any());
        deploy(uri, true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            consumer.poll();
            assertEquals(Set.of("/a.xml", "/z.xml"), new HashSet<>(deliveries));
            assertEquals(2, deliveries.size());
            assertFalse(Files.exists(first));
            assertFalse(Files.exists(last));
            assertEquals("<refused/>", Files.readString(refused));
            // The refused names stay in the listing, so the sweep does not take the marker as an orphan.
            assertTrue(Files.exists(marker));
            // The same names fail on each poll. The repeat limit of the endpoint log keeps one warning per name.
            List<String> warnings = logs.messages(Level.WARNING, endpoint, "Could not read the type of a listed file");
            assertEquals(2, warnings.size());
            assertEquals(1, warnings.stream().filter(line -> line.contains("refused.xml")).count());
            assertEquals(1, warnings.stream().filter(line -> line.contains("ghost.xml")).count());
        } finally {
            close();
        }

        deliveries.clear();
        Path waiting = write("c.xml", "<c/>", 1);
        fault.set(Fault.TRANSPORT);
        deploy(uri, true);
        try (Logs logs = new Logs()) {
            consumer.poll();
            assertTrue(deliveries.isEmpty());
            assertEquals("<c/>", Files.readString(waiting));
            assertEquals("<refused/>", Files.readString(refused));
            assertTrue(logs.messages(Level.WARNING, endpoint, "Could not read the type of a listed file").isEmpty());
        }
    }

    @Test
    void reconnectWaitUsesTheParsedIntervalForEachResolve() throws Exception {
        String uri = folder.toUri().toString();
        String key = "transport.vfs.ReconnectTimeout";
        properties.setProperty("transport.vfs.MaxRetryCount", "2");
        String[] inputs = {null, "30", "30000", "999", "1000", "1000s", "500ms", "500"};
        long[] expected = {30000, 30000, 30000, 999000, 1000, 1000000, 500, 500000};
        String clock = "pollwait." + endpoint;
        for (int index = 0; index < inputs.length; index++) {
            String input = inputs[index];
            if (input != null) {
                properties.setProperty(key, input);
            }
            AtomicLong elapsed = new AtomicLong();
            List<Long> attempts = new ArrayList<>();
            setupSession = session -> doAnswer(call -> {
                attempts.add(elapsed.get());
                throw new FileSystemException("The resolve fixture is unavailable.");
            }).when(session).resolve(eq(uri), any());
            try (Logs logs = new Logs(); MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
                clocks.when(() -> TestPoints.nanoTime(clock)).thenAnswer(call ->
                        TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
                clocks.when(() -> TestPoints.slice(eq(clock), anyLong())).thenAnswer(call -> {
                    elapsed.addAndGet(call.getArgument(1));
                    return 1L;
                });
                deploy(uri, true);
                consumer.poll();
                assertEquals(List.of(0L, expected[index]), attempts);
                List<String> warnings = logs.messages(Level.WARNING, endpoint, key);
                boolean bare = input != null && input.matches("[0-9]+");
                assertEquals(bare ? 1 : 0, warnings.size());
                if (bare) {
                    assertTrue(warnings.get(0).contains(input));
                    assertTrue(warnings.get(0).contains("unit"));
                    assertTrue(warnings.get(0).contains(input + (Long.parseLong(input) < 1000 ? " s" : " ms")));
                }
            } finally {
                close();
            }
        }
        for (String invalid : List.of("abc", "-5", "10m")) {
            properties.setProperty(key, invalid);
            assertTrue(assertThrows(IllegalArgumentException.class, () -> deploy(uri, true)).getMessage().contains(key));
        }
    }

    @Test
    void destroyWakesThePendingReconnectWithoutASecondResolve() throws Exception {
        String uri = folder.toUri().toString();
        properties.setProperty("transport.vfs.MaxRetryCount", "2");
        properties.setProperty("transport.vfs.ReconnectTimeout", "30");
        CountDownLatch entered = new CountDownLatch(1);
        AtomicLong attempts = new AtomicLong();
        setupSession = session -> doAnswer(call -> {
            attempts.incrementAndGet();
            entered.countDown();
            throw new FileSystemException("The resolve fixture is unavailable.");
        }).when(session).resolve(eq(uri), any());
        deploy(uri, true);
        Thread poller = new Thread(consumer::poll);
        poller.start();
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            Thread.sleep(1000);
            long start = System.nanoTime();
            consumer.destroy();
            poller.join(1500);
            assertFalse(poller.isAlive());
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1500);
            assertEquals(1L, attempts.get());
        } finally {
            consumer.destroy();
            poller.join(2000);
        }
    }

    @Test
    void lockedDecisionCountsAReplacementAndLeavesARetryOutsideTheCount() throws Exception {
        for (boolean pending : new boolean[]{false, true}) {
            for (boolean replacement : new boolean[]{false, true}) {
                verifyCount(pending, replacement, true);
            }
        }
        verifyCount(false, true, false);
    }

    private void verifyCount(boolean pending, boolean replacement, boolean sequential) throws Exception {
        Path input = Files.createDirectory(folder.resolve("input" + pending + replacement + sequential));
        Path unavailable = Files.writeString(input.resolveSibling(input.getFileName() + ".txt"), "ordinary file");
        Path first = Files.writeString(input.resolve("a.xml"), "<a/>");
        Path marker = input.resolve("a.xml.fail");
        java.util.concurrent.atomic.AtomicBoolean withholdWrite = new java.util.concurrent.atomic.AtomicBoolean();
        setupSession = session -> doAnswer(call -> {
            org.wso2.vfs.shared.lock.LockIo io = spy((org.wso2.vfs.shared.lock.LockIo) call.callRealMethod());
            doAnswer(look -> {
                String uri = look.getArgument(0);
                if (withholdWrite.get() && uri.contains("a.xml.fail.") && uri.endsWith(".next")) {
                    throw new FileSystemException("Local state write fixture");
                }
                return look.callRealMethod();
            }).when(io).resolve(anyString());
            return io;
        }).when(session).lockIo(anyString());
        properties.setProperty("transport.vfs.FileProcessCount", "1");
        properties.setProperty("transport.vfs.FileSortAttribute", "Name");
        properties.setProperty("transport.vfs.FileSortAscending", "true");
        properties.setProperty("transport.vfs.ActionAfterProcess", "MOVE");
        properties.setProperty("transport.vfs.MoveAfterProcess", unavailable.toUri().toString());
        properties.setProperty(VFSConstants.TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION, "0");
        deliveries.clear();
        deploy(input.toUri().toString(), sequential);
        try {
            consumer.poll();
            assertEquals(List.of("/a.xml"), deliveries);
            assertTrue(Files.isRegularFile(marker), "The first failed move must have a durable record.");
            if (replacement) {
                Files.writeString(first, "<replacement/>");
            }
            Files.writeString(input.resolve("b.xml"), "<b/>");
            withholdWrite.set(pending);
            deliveries.clear();
            consumer.poll();
            if (replacement && sequential) {
                assertEquals(List.of("/a.xml"), deliveries);
                assertFalse(Files.exists(input.resolve("b.xml.fail")));
            } else {
                assertEquals(List.of("/b.xml"), deliveries);
                assertTrue(Files.isRegularFile(input.resolve("b.xml.fail")));
            }
            if (sequential) {
                assertEquals(pending, Files.exists(input.resolve("a.xml.lock")));
                assertEquals(!pending || !replacement, Files.exists(marker));
            } else {
                assertFalse(Files.exists(marker));
                deliveries.clear();
                consumer.poll();
                assertEquals(List.of("/a.xml"), deliveries);
            }
        } finally {
            withholdWrite.set(false);
            close();
        }
    }


    @Test
    void skippedTicksPreserveTheLastObservationAndSuccessfulListingsAdvanceIt() throws Exception {
        String uri = folder.toUri().toString();
        AtomicLong elapsed = new AtomicLong();
        AtomicReference<FileSystemException> failure = new AtomicReference<>();
        AtomicLong resolves = new AtomicLong();
        setupSession = session -> doAnswer(call -> {
            resolves.incrementAndGet();
            FileSystemException local = failure.getAndSet(null);
            if (local != null) {
                throw local;
            }
            return call.callRealMethod();
        }).when(session).resolve(eq(uri), any());
        properties.setProperty("transport.vfs.MaxRetryCount", "1");
        String clock = "pollwait." + endpoint;
        try (MockedStatic<TestPoints> clocks = mockStatic(TestPoints.class, CALLS_REAL_METHODS)) {
            clocks.when(() -> TestPoints.nanoTime(clock)).thenAnswer(call -> TimeUnit.MILLISECONDS.toNanos(elapsed.get()));
            deploy(uri, true);
            EndpointHealth health = VFSConsumerLifecycleTest.field(consumer, "health", EndpointHealth.class);
            when(configuration.getSequence("process")).thenReturn(null);
            consumer.poll();
            assertEquals(0L, sampleHealth(health).get("lastSuccess"));
            assertEquals(0L, resolves.get());
            when(configuration.getSequence("process")).thenReturn(sequence);
            consumer.poll();
            Map<String, Object> empty = sampleHealth(health);
            long emptyAt = ((Number) empty.get("lastSuccess")).longValue();
            assertTrue(emptyAt > 0);
            assertEquals(0L, empty.get("filesDone"));
            Path queued = write("waiting.xml", "<q/>", 1);
            failure.set(new FileSystemException(new java.net.SocketException("Local transport control")));
            consumer.poll();
            Map<String, Object> failed = sampleHealth(health);
            assertEquals("DOWN", failed.get("state"));
            assertEquals(emptyAt, failed.get("lastSuccess"));
            long attempted = resolves.get();
            consumer.poll();
            assertEquals(attempted, resolves.get());
            assertTrue(Files.isRegularFile(queued));
            assertEquals(failed, sampleHealth(health));
            when(configuration.getSequence("process")).thenReturn(null);
            elapsed.addAndGet(1001);
            consumer.poll();
            assertEquals(attempted, resolves.get());
            assertEquals(failed, sampleHealth(health));
            when(configuration.getSequence("process")).thenReturn(sequence);
            consumer.poll();
            Map<String,Object> recovered = sampleHealth(health);
            assertEquals("OK", recovered.get("state"));
            assertTrue(((Number) recovered.get("lastSuccess")).longValue() > emptyAt);
            assertEquals(1L, recovered.get("filesDone"));
            assertEquals(failed.get("filesFailed"), recovered.get("filesFailed"));
            assertEquals(failed.get("transportFaults"), recovered.get("transportFaults"));
            assertEquals(failed.get("lastError"), recovered.get("lastError"));
            assertEquals(List.of("/waiting.xml"), deliveries);
            assertFalse(Files.exists(queued));
        }
    }


    @ParameterizedTest
    @ValueSource(strings = {"default", "parameter", "header", "file", "append", "missing", "denied", "otherThread"})
    @SuppressWarnings("unchecked")
    void respondOnThePollingPath(String scenario) throws Exception {
        Path input = Files.createDirectory(folder.resolve("inbox"));
        Path source = Files.write(input.resolve("sample.xml"), "<receipt>body</receipt>".getBytes(StandardCharsets.UTF_8));
        Path replies = Files.createDirectory(folder.resolve("replies"));
        String target = "response.xml";
        if (!scenario.equals("missing")) {
            Path location = replies;
            if (scenario.equals("file")) {
                target = "explicit.xml";
                location = replies.resolve(target);
            } else if (scenario.equals("denied")) {
                location = source.resolve("impossible");
            }
            properties.setProperty(VFSConstants.REPLY_FILE_URI, location.toUri().toString());
        }
        if (scenario.equals("parameter") || scenario.equals("header")) {
            properties.setProperty(VFSConstants.REPLY_FILE_NAME, "parameter.xml");
            target = scenario.equals("header") ? "header.xml" : "parameter.xml";
        }
        if (scenario.equals("append")) {
            properties.setProperty(VFSConstants.APPEND, "true");
            Files.write(replies.resolve(target), "prefix".getBytes(StandardCharsets.UTF_8));
        }
        boolean expectFailure = scenario.equals("missing") || scenario.equals("denied") || scenario.equals("otherThread");
        mediation = message -> {
            Map<?, ?> metadata = (Map<?, ?>) message.getProperty(SynapseConstants.STATISTICS_METADATA);
            assertEquals("file", metadata.get("protocol"));
            assertTrue(metadata.get("fileURI").toString().endsWith("/inbox/sample.xml"));
            InboundResponseSender sender = (InboundResponseSender)
                    message.getProperty(InboundEndpointConstants.INBOUND_ENDPOINT_RESPONSE_WORKER);
            assertNotNull(sender);
            MessageContext axis = message.getAxis2MessageContext();
            axis.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/xml");
            Map<String, Object> headers = (Map<String, Object>) axis.getProperty(MessageContext.TRANSPORT_HEADERS);
            if (scenario.equals("header")) {
                headers.put(VFSConstants.REPLY_FILE_NAME, "header.xml");
            }
            if (scenario.equals("otherThread")) {
                java.util.concurrent.CompletableFuture.runAsync(() ->
                        assertThrows(SynapseException.class, () -> sender.sendBack(message))).join();
            } else if (expectFailure) {
                assertThrows(SynapseException.class, () -> sender.sendBack(message));
            } else {
                sender.sendBack(message);
            }
            assertEquals(expectFailure, headers.containsKey(org.apache.synapse.commons.vfs.VFSConstants.ERROR_CODE));
        };
        deploy(input.toUri().toString(), true);
        consumer.poll();
        assertEquals(1, deliveries.size());
        assertEquals(expectFailure, Files.exists(source));
        try (java.util.stream.Stream<Path> files = Files.walk(replies)) {
            assertEquals(expectFailure ? 0 : 1, files.filter(Files::isRegularFile).count());
        }
        if (!expectFailure) {
            String body = new String(Files.readAllBytes(replies.resolve(target)), StandardCharsets.UTF_8);
            assertTrue(body.contains("<receipt>body</receipt>"), body);
            assertEquals(scenario.equals("append"), body.startsWith("prefix"));
        }
        Map<String, Object> snapshot = consumer.getHealth();
        assertEquals(EndpointHealth.State.valueOf((String) snapshot.get("state")).status(), snapshot.get("status"));
        assertEquals(expectFailure ? 1L : 0L, snapshot.get("filesFailed"));
        assertEquals(expectFailure ? 0L : 1L, snapshot.get("filesDone"));
    }

    @ParameterizedTest
    @CsvSource({
            "nested/response.xml,nested/response.xml,false,false,false",
            "nested/response.xml,nested/response.xml,true,true,false",
            "how%3Fwhen.xml,how?when.xml,false,true,false",
            "how%3Fwhen.xml,how?when.xml,true,false,false",
            "sub/how%3Fwhen%2525.xml,sub/how?when%25.xml,false,false,false",
            "sub/how%3Fwhen%2525.xml,sub/how?when%25.xml,true,true,false",
            "how?when.xml,how,false,false,false",
            "sum%2525.xml,sum%25.xml,true,false,false",
            "nested/response.xml,nested/response.xml,false,true,true",
            "how%3Fwhen.xml,how?when.xml,true,true,true"})
    void replyTargetKeepsItsName(String supplied, String filename, boolean append, boolean header,
                                  boolean explicit) throws Exception {
        Path input = Files.createDirectory(folder.resolve("input"));
        Files.writeString(input.resolve("source.xml"), "<receipt>body</receipt>");
        Path replies = Files.createDirectory(folder.resolve("replies"));
        Path expected = replies.resolve(filename);
        Files.createDirectories(expected.getParent());
        Files.writeString(expected, "old");
        properties.setProperty(VFSConstants.REPLY_FILE_URI, (explicit ? expected : replies).toUri().toString());
        properties.setProperty(VFSConstants.REPLY_FILE_NAME, header ? "parameter.xml" : supplied);
        properties.setProperty(VFSConstants.APPEND, Boolean.toString(append));
        mediation = message -> {
            MessageContext axis = message.getAxis2MessageContext();
            axis.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/xml");
            if (header) {
                @SuppressWarnings("unchecked")
                Map<String, Object> headers = (Map<String, Object>) axis.getProperty(MessageContext.TRANSPORT_HEADERS);
                headers.put(VFSConstants.REPLY_FILE_NAME, supplied);
            }
            ((InboundResponseSender) message.getProperty(InboundEndpointConstants.INBOUND_ENDPOINT_RESPONSE_WORKER))
                    .sendBack(message);
        };
        deploy(input.toUri().toString(), !append);
        consumer.poll();
        assertEquals(1L, consumer.getHealth().get("filesDone"));
        assertEquals(0L, consumer.getHealth().get("filesFailed"));
        String body = Files.readString(expected);
        assertEquals(append, body.startsWith("old"));
        assertTrue(body.contains("<receipt>body</receipt>"), body);
        try (java.util.stream.Stream<Path> files = Files.walk(replies)) {
            assertEquals(1L, files.filter(Files::isRegularFile).count());
        }
    }

    @Test
    void twoWorkersWaitForEachOtherOnOneFixedReplyName() throws Exception {
        Path input = Files.createDirectory(folder.resolve("input"));
        Files.writeString(input.resolve("a.xml"), "<receipt>a</receipt>");
        Files.writeString(input.resolve("b.xml"), "<receipt>b</receipt>");
        Path replies = Files.createDirectory(folder.resolve("replies"));
        properties.setProperty(VFSConstants.REPLY_FILE_URI, replies.toUri().toString());
        properties.setProperty("transport.vfs.WorkerPoolSize", "2");
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        // The reply of a.xml holds the lock of response.xml inside its write until b.xml has started its reply.
        ApplicationXMLFormatter held = new ApplicationXMLFormatter() {
            @Override
            public void writeTo(MessageContext context, OMOutputFormat format, OutputStream out, boolean preserve)
                    throws AxisFault {
                writing.countDown();
                try {
                    assertTrue(released.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    throw new AxisFault("Interrupted.", interrupted);
                }
                super.writeTo(context, format, out, preserve);
            }
        };
        mediation = message -> {
            MessageContext axis = message.getAxis2MessageContext();
            Map<?, ?> headers = (Map<?, ?>) axis.getProperty(MessageContext.TRANSPORT_HEADERS);
            InboundResponseSender sender = (InboundResponseSender)
                    message.getProperty(InboundEndpointConstants.INBOUND_ENDPOINT_RESPONSE_WORKER);
            if ("a.xml".equals(headers.get(VFSConstants.FILE_NAME))) {
                axis.getConfigurationContext().getAxisConfiguration().addMessageFormatter("application/held", held);
                axis.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/held");
            } else {
                axis.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/xml");
                try {
                    assertTrue(writing.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    throw new AssertionError(interrupted);
                }
                new Thread(() -> {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    released.countDown();
                }).start();
            }
            sender.sendBack(message);
        };
        deploy(input.toUri().toString(), false);
        consumer.poll();
        assertEquals(2L, consumer.getHealth().get("filesDone"));
        assertEquals(0L, consumer.getHealth().get("filesFailed"));
        assertFalse(Files.exists(input.resolve("a.xml")));
        assertFalse(Files.exists(input.resolve("b.xml")));
        String body = Files.readString(replies.resolve("response.xml"));
        assertTrue(body.contains("<receipt>b</receipt>"), body);
    }

    private Map<String, Object> sampleHealth(EndpointHealth health) throws Exception {
        // Normally the health ticker publishes the state. Sample it now on the fixture clock.
        java.lang.reflect.Method tick = EndpointHealth.class.getDeclaredMethod("refresh", boolean.class);
        tick.setAccessible(true);
        tick.invoke(health, false);
        return health.snapshot();
    }

    @Test
    void ordinarySiblingNamesKeepIndependentWorkerRecords() throws Exception {
        Path input = Files.createDirectory(folder.resolve("in"));
        Path target = Files.writeString(folder.resolve("file-instead-of-folder"), "ordinary file");
        List<String> names = List.of("x.csv", "x.csv.next");
        for (String name : names) {
            Files.writeString(input.resolve(name), "<sample/>");
        }
        properties.setProperty("transport.vfs.WorkerPoolSize", "2");
        properties.setProperty("transport.vfs.ActionAfterProcess", "MOVE");
        properties.setProperty("transport.vfs.MoveAfterProcess", target.toUri().toString());
        deploy(input.toUri().toString(), false);
        consumer.poll();
        consumer.poll();
        consumer.poll();
        for (String name : names) {
            assertEquals(1, Collections.frequency(deliveries, "/" + name));
            Path source = input.resolve(name);
            byte[] stored = Files.readAllBytes(input.resolve(name + ".fail"));
            FailState record = FailState.parse(stored, source.toUri().toString(), name);
            assertNotNull(record);
            assertEquals(name, record.name);
            assertTrue(Files.isRegularFile(source));
            assertFalse(Files.exists(input.resolve(name + ".lock")));
        }
        assertEquals(names.size(), deliveries.size());
    }

    @Test
    void configuredSingleFileKeepsItsPathEscapesAndOptionsThroughAdmission() throws Exception {
        for (boolean sequential : new boolean[] {true, false}) {
            for (String name : List.of("plain.xml", "x?y.xml", "x%3Fy.xml", "two words.xml")) {
                Path input = Files.createTempDirectory(folder, "single");
                Path target = Files.writeString(input.resolve(name), "<target/>");
                Path neighbor = Files.writeString(input.resolve("x"), "<neighbor/>");
                String uri = target.toUri() + "?transport.vfs.AvoidPermissionCheck=true";
                java.util.concurrent.CopyOnWriteArrayList<String> lookups = new java.util.concurrent.CopyOnWriteArrayList<>();
                setupSession = value -> doAnswer(call -> {
                    org.wso2.vfs.shared.lock.LockIo io = spy((org.wso2.vfs.shared.lock.LockIo) call.callRealMethod());
                    doAnswer(read -> {
                        lookups.add(read.getArgument(0));
                        return read.callRealMethod();
                    }).when(io).resolve(anyString());
                    return io;
                }).when(value).lockIo(anyString());
                deliveries.clear();
                deploy("vfs:" + uri, sequential);
                try {
                    consumer.poll();
                    assertEquals(1, deliveries.size(), name);
                    assertFalse(Files.exists(target), name);
                    assertEquals("<neighbor/>", Files.readString(neighbor), name);
                    assertTrue(lookups.contains(uri), "Original data URI must reach LockIo: " + name);
                    String lock = target.toUri() + ".lock?transport.vfs.AvoidPermissionCheck=true";
                    assertTrue(lookups.contains(lock), "Original path and options on its lock: " + name);
                    consumer.poll();
                    assertEquals(1, deliveries.size());
                } finally {
                    close();
                }
            }
        }
    }
}
