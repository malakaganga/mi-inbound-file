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

import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.carbon.inbound.vfs.reply.ReplySender;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.VFS;
import org.wso2.vfs.shared.failstate.DigestingInputStream;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.log.RepeatLimiter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
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
    // of the provider does. A type in upper case, a type with no builder and no type with an extension other than xml
    // reach the SOAP builder, which gives no element for an empty file. The transport makes an empty envelope of that.
    @ParameterizedTest
    @CsvSource({"application/xml,e.xml,false,false", "application/xml,e.xml,false,true",
            "application/xml,e.xml,true,false", "application/xml,e.xml,true,true",
            "APPLICATION/SOAP+XML,e.txt,false,false", "APPLICATION/SOAP+XML,e.txt,false,true",
            "APPLICATION/SOAP+XML,e.txt,true,false", "APPLICATION/SOAP+XML,e.txt,true,true",
            "application/soap+xml,e.txt,false,false", "application/soap+xml,e.txt,true,false",
            "application/x-unknown,e.txt,false,false", "application/x-unknown,e.txt,false,true",
            "application/x-unknown,e.txt,true,false", "application/x-unknown,e.txt,true,true",
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
}
