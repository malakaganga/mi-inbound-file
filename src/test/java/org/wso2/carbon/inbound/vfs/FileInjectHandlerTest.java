/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.carbon.inbound.vfs;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPFactory;
import org.apache.axis2.Constants;
import org.apache.axis2.AxisFault;
import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.format.PlainTextBuilder;
import org.apache.axis2.format.PlainTextFormatter;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.transport.http.ApplicationXMLFormatter;
import org.apache.axis2.transport.http.SOAPMessageFormatter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.carbon.inbound.vfs.reply.ReplySender;
import org.mockito.ArgumentCaptor;
import org.wso2.org.apache.commons.vfs2.FileContent;
import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.VFS;
import org.wso2.vfs.shared.failstate.DigestingInputStream;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.log.RepeatLimiter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The content of one file on its way into mediation.
 */
class FileInjectHandlerTest {

    // The SHA-256 of zero bytes.
    private static final String EMPTY_DIGEST = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private static final String ENDPOINT = "inject";

    @TempDir
    Path folder;

    // An empty file builds an empty envelope and goes into mediation once. The content is read through the real
    // digest and record streams, from a provider stream that closes itself at the end of the file, as the SFTP stream
    // of the provider does. A SOAP type in upper case, a type with no builder in any case, and no type with an extension
    // other than xml and txt reach the SOAP builder as a type that it does not take as SOAP or REST. It then gives no
    // element for an empty file, and the transport makes an empty envelope of that.
    @ParameterizedTest
    @CsvSource({"application/xml,e.xml,false,false", "application/xml,e.xml,false,true",
            "application/xml,e.xml,true,false", "application/xml,e.xml,true,true",
            "APPLICATION/SOAP+XML,e.txt,false,false", "APPLICATION/SOAP+XML,e.txt,false,true",
            "APPLICATION/SOAP+XML,e.txt,true,false", "APPLICATION/SOAP+XML,e.txt,true,true",
            "TEXT/XML,e.txt,false,false", "TEXT/XML,e.txt,true,false",
            "application/soap+xml,e.txt,false,false", "application/soap+xml,e.txt,true,false",
            "application/soap+xml; charset=UTF-8,e.txt,true,false", "text/xml,e.txt,true,false",
            "text/xml; charset=UTF-8,e.txt,true,false",
            "application/x-unknown,e.txt,false,false", "application/x-unknown,e.txt,false,true",
            "application/x-unknown,e.txt,true,false", "application/x-unknown,e.txt,true,true",
            "APPLICATION/X-UNKNOWN,e.txt,true,false",
            "'',e.dat,false,false", "'',e.dat,false,true", "'',e.dat,true,false", "'',e.dat,true,true",
            "'',e.xml,false,false", "'',e.xml,true,false"})
    void emptyFileIsMediatedOnceWithAnEmptyBody(String type, String name, boolean build, boolean streaming)
            throws Exception {
        Path empty = Files.createFile(folder.resolve(name));
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        if (!type.isEmpty()) {
            properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE, type);
        }
        properties.setProperty(VFSConstants.TRANSPORT_BUILD, String.valueOf(build));
        properties.setProperty(VFSConstants.STREAMING, String.valueOf(streaming));

        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SequenceMediator sequence = mock(SequenceMediator.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        List<SOAPEnvelope> injected = new ArrayList<>();
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenAnswer(call -> {
            injected.add(((Axis2MessageContext) call.getArgument(0)).getEnvelope());
            return true;
        });

        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        NamedLog log = NamedLog.inbound(LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter());
        FileObject file = VFS.getManager().resolveFile(empty.toUri().toString());
        FileIdentity identity = new FileIdentity(file.getName().getURI(), 0,
                Files.getLastModifiedTime(empty).toMillis(), FileIdentity.TimeSource.LISTING, null);
        FileInjectHandler handler = new FileInjectHandler("process", null, environment, new VFSConfig(properties));
        try (FileInjectHandler.Content content = new FileInjectHandler.Content(file, identity, store,
                mock(Generation.class), mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            assertTrue(handler.invoke(sequence, ENDPOINT, new HashMap<>(), content));
            assertNull(content.readFailure());
        } finally {
            file.close();
        }
        assertEquals(1, injected.size());
        assertNull(injected.get(0).getBody().getFirstElement());
        assertEquals(EMPTY_DIGEST, identity.digest());
    }

    // A file that the builder refuses has its own ERROR line, which names the file with the password masked. Two
    // files refused with the same cause give two lines. The same file again in the window of the repeat limiter gives
    // no new line.
    @Test
    void aRefusedFileIsNamedInItsOwnLine() throws Exception {
        Log out = mock(Log.class);
        when(out.isErrorEnabled()).thenReturn(true);
        NamedLog log = NamedLog.inbound(out, ENDPOINT, new RepeatLimiter());
        for (String name : new String[]{"a.xml", "b.xml", "a.xml"}) {
            assertFalse(refuse(name, log));
        }
        ArgumentCaptor<Object> lines = ArgumentCaptor.forClass(Object.class);
        verify(out, times(2)).error(lines.capture());
        String first = lines.getAllValues().get(0).toString();
        String second = lines.getAllValues().get(1).toString();
        String line = "[inbound:" + ENDPOINT + "] File mediation failed for sftp://user:***@host/in/";
        assertTrue(first.startsWith(line + "a.xml."), first);
        assertTrue(second.startsWith(line + "b.xml."), second);
        assertFalse(first.contains("secret"), first);
        assertFalse(second.contains("secret"), second);
    }

    // A file that fails on every poll gives one line in the window of the repeat limiter. The first line after the
    // window carries the count of the lines held back.
    @Test
    @Tag("testbuild")
    void aFileThatFailsOnEveryPollIsNamedOncePerWindow() throws Exception {
        String previous = System.setProperty("vfs.testpoints.dir", folder.toString());
        try {
            Log out = mock(Log.class);
            when(out.isErrorEnabled()).thenReturn(true);
            NamedLog log = NamedLog.inbound(out, ENDPOINT, new RepeatLimiter());
            for (int poll = 0; poll < 4; poll++) {
                if (poll == 3) {
                    try (Stream<Path> children = Files.list(folder)) {
                        Path copy = children.filter(Files::isDirectory).findFirst().get();
                        Files.write(copy.resolve("clock.repeat.mono"), "300010".getBytes(StandardCharsets.UTF_8));
                    }
                }
                assertFalse(refuse("a.xml", log));
            }
            ArgumentCaptor<Object> lines = ArgumentCaptor.forClass(Object.class);
            verify(out, times(2)).error(lines.capture());
            String last = lines.getAllValues().get(1).toString();
            assertTrue(last.contains("File mediation failed for sftp://user:***@host/in/a.xml."), last);
            assertTrue(last.endsWith(" (skipped 2)"), last);
        } finally {
            if (previous == null) {
                System.clearProperty("vfs.testpoints.dir");
            } else {
                System.setProperty("vfs.testpoints.dir", previous);
            }
        }
    }

    // Mediates one file of an SFTP folder whose content is not XML, so the XML builder refuses it.
    private boolean refuse(String name, NamedLog log) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, "sftp://user:secret@host/in");
        properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE, "application/xml");
        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SequenceMediator sequence = mock(SequenceMediator.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        FileName fileName = VFS.getManager().resolveURI("sftp://user:secret@host/in/" + name);
        FileObject file = mock(FileObject.class);
        FileContent content = mock(FileContent.class);
        when(file.getName()).thenReturn(fileName);
        when(file.getContent()).thenReturn(content);
        when(content.getInputStream()).thenAnswer(call ->
                new ByteArrayInputStream("not xml".getBytes(StandardCharsets.UTF_8)));
        FileIdentity identity = new FileIdentity(fileName.getURI(), 7, 0, FileIdentity.TimeSource.LISTING, null);
        FileInjectHandler handler = new FileInjectHandler("process", null, environment, new VFSConfig(properties));
        try (FileInjectHandler.Content files = new FileInjectHandler.Content(file, identity, store,
                mock(Generation.class), mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            boolean result = handler.invoke(sequence, ENDPOINT, new HashMap<>(), files);
            assertNull(files.readFailure());
            return result;
        }
    }

    // A type with a charset, as the transport listener reads it. The charset is on the axis2 context, where the
    // builder and the formatter read it, so the file is decoded with it and written in it again. A type with no
    // charset leaves it unset, and the content is UTF-8.
    @ParameterizedTest
    @CsvSource({"text/plain,ISO-8859-1,false", "text/plain,ISO-8859-1,true", "text/plain,UTF-16,false",
            "text/plain,UTF-16,true", "application/xml,ISO-8859-1,false", "application/xml,ISO-8859-1,true",
            "application/xml,UTF-16,false", "application/xml,UTF-16,true", "text/xml,ISO-8859-1,false",
            "text/xml,ISO-8859-1,true", "text/xml,UTF-16,false", "text/xml,UTF-16,true", "text/plain,'',false",
            "text/plain,'',true", "application/xml,'',true", "text/xml,'',true"})
    void charsetOfTheTypeIsOnTheAxis2Context(String type, String charset, boolean build) throws Exception {
        Charset encoding = charset.isEmpty() ? StandardCharsets.UTF_8 : Charset.forName(charset);
        String text = "caf\u00e9 \u00fcber";
        String envelope = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><a>" + text
                + "</a></s:Body></s:Envelope>";
        String content = "text/plain".equals(type) ? text : "text/xml".equals(type) ? envelope : "<a>" + text + "</a>";
        Path file = Files.write(folder.resolve("c.dat"), content.getBytes(encoding));
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE,
                charset.isEmpty() ? type : type + "; charset=" + charset);
        properties.setProperty(VFSConstants.TRANSPORT_BUILD, String.valueOf(build));
        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SequenceMediator sequence = mock(SequenceMediator.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        axis.addMessageBuilder("text/plain", new PlainTextBuilder());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        MessageFormatter formatter = "text/plain".equals(type) ? new PlainTextFormatter()
                : "text/xml".equals(type) ? new SOAPMessageFormatter() : new ApplicationXMLFormatter();
        List<Object> property = new ArrayList<>();
        ByteArrayOutputStream written = new ByteArrayOutputStream();
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenAnswer(call -> {
            MessageContext message = ((Axis2MessageContext) call.getArgument(0)).getAxis2MessageContext();
            property.add(message.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
            formatter.writeTo(message, BaseUtils.getOMOutputFormat(message), written, false);
            return true;
        });
        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        NamedLog log = NamedLog.inbound(LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter());
        FileObject source = VFS.getManager().resolveFile(file.toUri().toString());
        FileIdentity identity = new FileIdentity(source.getName().getURI(), Files.size(file),
                Files.getLastModifiedTime(file).toMillis(), FileIdentity.TimeSource.LISTING, null);
        FileInjectHandler handler = new FileInjectHandler("process", null, environment, new VFSConfig(properties));
        try (FileInjectHandler.Content files = new FileInjectHandler.Content(source, identity, store,
                mock(Generation.class), mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            assertTrue(handler.invoke(sequence, ENDPOINT, new HashMap<>(), files));
        } finally {
            source.close();
        }
        assertEquals(1, property.size());
        assertEquals(charset.isEmpty() ? null : charset, property.get(0));
        String declaration = "text/xml".equals(type)
                ? "<?xml version='1.0' encoding='" + (charset.isEmpty() ? "UTF-8" : charset) + "'?>" : "";
        assertArrayEquals((declaration + content).getBytes(encoding), written.toByteArray(),
                new String(written.toByteArray(), encoding));
    }

    // A new message of Synapse has a SOAP 1.2 envelope, as Axis2SynapseEnvironment gives it. The file is built as on
    // the transport, on a SOAP 1.1 message. So the relay builder of the pack, which asks the message for its version,
    // builds SOAP 1.1 under application/octet-stream and application/pdf. A SOAP file keeps its own version.
    @ParameterizedTest
    @CsvSource({"application/octet-stream,bytes,http://schemas.xmlsoap.org/soap/envelope/",
            "application/pdf,bytes,http://schemas.xmlsoap.org/soap/envelope/",
            "application/soap+xml,http://www.w3.org/2003/05/soap-envelope,http://www.w3.org/2003/05/soap-envelope",
            "text/xml,http://schemas.xmlsoap.org/soap/envelope/,http://schemas.xmlsoap.org/soap/envelope/"})
    void fileIsBuiltOnASoap11Message(String type, String content, String namespace) throws Exception {
        String text = content.startsWith("http") ? "<s:Envelope xmlns:s=\"" + content + "\"><s:Body><a/></s:Body>"
                + "</s:Envelope>" : "some bytes";
        Path file = Files.write(folder.resolve("f.dat"), text.getBytes(StandardCharsets.UTF_8));
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE, type);
        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SequenceMediator sequence = mock(SequenceMediator.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/octet-stream", new RelayLikeBuilder());
        axis.addMessageBuilder("application/pdf", new RelayLikeBuilder());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            message.setEnvelope(OMAbstractFactory.getSOAP12Factory().getDefaultEnvelope());
            return new Axis2MessageContext(message, configuration, environment);
        });
        List<SOAPEnvelope> injected = new ArrayList<>();
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenAnswer(call -> {
            injected.add(((Axis2MessageContext) call.getArgument(0)).getEnvelope());
            return true;
        });
        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        NamedLog log = NamedLog.inbound(LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter());
        FileObject source = VFS.getManager().resolveFile(file.toUri().toString());
        FileIdentity identity = new FileIdentity(source.getName().getURI(), Files.size(file),
                Files.getLastModifiedTime(file).toMillis(), FileIdentity.TimeSource.LISTING, null);
        FileInjectHandler handler = new FileInjectHandler("process", null, environment, new VFSConfig(properties));
        try (FileInjectHandler.Content files = new FileInjectHandler.Content(source, identity, store,
                mock(Generation.class), mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            assertTrue(handler.invoke(sequence, ENDPOINT, new HashMap<>(), files));
        } finally {
            source.close();
        }
        assertEquals(1, injected.size());
        assertEquals(namespace, injected.get(0).getNamespace().getNamespaceURI());
    }

    // Builds as the relay builder of the pack does (org.wso2.carbon.relay.BinaryRelayBuilder): the envelope takes the
    // SOAP version of the message, with the content in a binary element.
    private static final class RelayLikeBuilder implements Builder {
        @Override
        public OMElement processDocument(InputStream input, String type, MessageContext message) throws AxisFault {
            try {
                while (input.read() >= 0) {
                    // The content is read and dropped. The test looks at the envelope only.
                }
            } catch (IOException failure) {
                throw AxisFault.makeFault(failure);
            }
            SOAPFactory factory = message.isSOAP11() ? OMAbstractFactory.getSOAP11Factory()
                    : OMAbstractFactory.getSOAP12Factory();
            SOAPEnvelope envelope = factory.getDefaultEnvelope();
            envelope.getBody().addChild(factory.createOMElement("binary",
                    factory.createOMNamespace("http://ws.apache.org/commons/ns/payload", "ns")));
            return envelope;
        }
    }
}
