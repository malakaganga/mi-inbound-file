/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.vfs;

import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.TransportInDescription;
import org.apache.synapse.commons.crypto.CryptoUtil;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.wso2.org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.wso2.org.apache.commons.vfs2.provider.sftp.SftpFileProvider;
import org.wso2.org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.provider.smb2.Smb2FileSystemConfigBuilder;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.log.NamedLog;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VFSConfigTest {

    private static final String PREFIX = "transport.vfs.";
    private static final String URI_KEY = PREFIX + "FileURI";
    private static final String TARGET = PREFIX + "MoveAfterProcess";
    private static final String AVOID = PREFIX + "AvoidPermissionCheck";

    private static Properties properties() {
        Properties p = new Properties();
        p.setProperty(URI_KEY, "sftp://host/in");
        p.setProperty("sequential", "false");
        return p;
    }

    private static Arguments row(String key, Function<VFSConfig, ?> read, String fallback, String input,
                                 String invalid, String expected) {
        return Arguments.of(PREFIX + key, read, fallback, input, invalid, expected);
    }

    static Stream<Arguments> parameters() {
        return Stream.of(
                row("ContentType", VFSConfig::getContentType, null, "application/xml", null, "application/xml"),
                row("FileNamePattern", VFSConfig::getFileNamePattern, null, ".+[.]csv", "[", ".+[.]csv"),
                row("ReplyFileURI", VFSConfig::getReplyFileURI, null, "file:///tmp/replies", "sftp:///missing", "file:///tmp/replies"),
                row("ReplyFileName", VFSConfig::getReplayFileName, "response.xml", "out.xml", null, "out.xml"),
                row("Streaming", VFSConfig::isStreaming, "false", "true", "text", "true"),
                row("Build", VFSConfig::isBuild, "false", "true", "text", "true"),
                row("Append", VFSConfig::isAppend, "false", "true", "text", "true"),
                row("FileProcessCount", VFSConfig::getFileProcessingCount, null, "3", "-1", "3"),
                row("FileProcessInterval", VFSConfig::getFileProcessingInterval, null, "25", "wrong", "25"),
                row("FileSortAttribute", VFSConfig::getFileSortParam, null, "Name", null, "Name"),
                row("FileSortAscending", VFSConfig::isFileSortAscending, "true", "false", "x", "false"),
                row("WorkerPoolSize", VFSConfig::getWorkerPoolSize, "4", "2", "0", "2"),
                row("FileSizeLimit", VFSConfig::getFileSizeLimit, "-1.0", "200.5", "NaN", "200.5"),
                row("MinimumAge", VFSConfig::getMinimumAge, null, "6", "x", "6"),
                row("MaximumAge", VFSConfig::getMaximumAge, null, "20", "x", "20"),
                row("CheckSizeInterval", VFSConfig::getCheckSizeInterval, "0", "16", "-1", "16"),
                row("CheckSizeIgnoreEmpty", VFSConfig::getCheckSizeIgnoreEmpty, "false", "true", "x", "true"),
                row("MaxReadTransportErrors", VFSConfig::getMaxReadTransportErrors, "3", "2", "0", "2"),
                row("ActionAfterProcess", VFSConfig::getActionAfterProcess, "0", "MOVE", "x", "1"),
                row("ActionAfterFailure", VFSConfig::getActionAfterFailure, "0", "NONE", "x", "2"),
                row("MoveAfterProcess", VFSConfig::getMoveAfterProcess, null, "sftp://host/done", "sftp:///x", "sftp://host/done"),
                row("MoveAfterFailure", VFSConfig::getMoveAfterFailure, null, "ftp://host/failure", "ftp:///x", "ftp://host/failure"),
                row("MoveAfterFailedMove", VFSConfig::getMoveAfterMoveFailure, null, "file:///tmp/fail", "broken", "file:///tmp/fail"),
                row("MoveTimestampFormat", VFSConfig::getMoveTimestampPattern, null, "yyyyMMdd", "'", "yyyyMMdd"),
                row("SubFolderTimestampFormat", VFSConfig::getSubfolderTimestamp, null, "yyyyMMdd", "'", "yyyyMMdd"),
                row("CreateFolder", VFSConfig::isForceCreateFolder, "false", "true", "bad", "true"),
                row("UpdateLastModified", VFSConfig::isUpdateLastModified, "true", "false", "x", "false"),
                row("TempFolderName", VFSConfig::getTempFolderName, ".part", ".partial", null, ".partial"),
                row("TempFileSuffix", VFSConfig::getTempFileSuffix, ".part", ".partial", null, ".partial"),
                row("MaxRetryCount", VFSConfig::getMaxRetryCount, "3", "7", "-1", "7"),
                row("ReconnectTimeout", VFSConfig::getReconnectTimeout, "30000", "999", "-1", "999000"),
                row("StopTimeout", VFSConfig::getStopTimeout, "60000", "8", "bad", "8000"),
                row("Locking", VFSConfig::isFileLocking, "true", "disable", null, "false"),
                row("AutoLockRelease", VFSConfig::isAutoLockRelease, "true", "false", "bad", "false"),
                row("LockStaleAge", VFSConfig::getLockStaleAge, "900000", "86400000", "-2", "86400000"),
                row("FailedRecordNextRetryDuration", VFSConfig::getFailedRecordNextRetryDuration, "30000", "250", "bad", "250"),
                row("FailedRecordFlagAfterRetries", VFSConfig::getFailedRecordFlagAfterRetries, "12", "5", "0", "5"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void checksParameterAtConstruction(String key, Function<VFSConfig, ?> read, String fallback, String input,
                                       String invalid, String expected) {
        Properties p = properties();
        Object actual = read.apply(new VFSConfig(p));
        assertEquals(fallback, actual == null ? null : actual.toString());
        if (key.equals(PREFIX + "ActionAfterProcess")) {
            p.setProperty(TARGET, "file:///done");
        }
        p.setProperty(key, input);
        VFSConfig config = new VFSConfig(p);
        p.setProperty(key, "changed");
        assertEquals(expected, read.apply(config).toString());
        if (invalid != null) {
            p.setProperty(key, invalid);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p));
            assertTrue(failure.getMessage().contains(key), failure.getMessage());
        }
    }

    @ParameterizedTest
    @CsvSource({"30,30000", "999,999000", "1000,1000", "30000,30000", "500ms,500", "1000s,1000000", "0,0"})
    void reconnectUnits(String text, long expected) {
        Properties p = properties();
        p.setProperty(PREFIX + "ReconnectTimeout", text);
        assertEquals(expected, new VFSConfig(p).getReconnectTimeout());
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "1m", "4.5s", "9223372036854776s", "bad"})
    void invalidDurationNamesParameter(String value) {
        Properties p = properties();
        String key = PREFIX + "ReconnectTimeout";
        p.setProperty(key, value);
        assertEquals("Invalid value for " + key + ".", assertThrows(IllegalArgumentException.class,
                () -> new VFSConfig(p)).getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Process", "Failure"})
    void moveRequiresItsOwnDestinationAtDeployment(String result) {
        Properties p = properties();
        String target = PREFIX + "MoveAfter" + result;
        p.setProperty(PREFIX + "ActionAfter" + result, "move");
        p.setProperty(PREFIX + "MoveAfter" + (result.equals("Process") ? "Failure" : "Process"), "file:///other");
        p.setProperty(PREFIX + "MoveAfterFailedMove", "file:///fallback");
        for (String missing : new String[]{null, "", "  "}) {
            if (missing != null) {
                p.setProperty(target, missing);
            }
            assertTrue(assertThrows(IllegalArgumentException.class, () ->
                    new VFSConsumer(p, "files", null, 1000, "main", "", true, true)).getMessage().contains(target));
            assertTrue(assertThrows(IllegalArgumentException.class, () ->
                    new VFSConsumer(p, "files", null, "0/5 * * * * ?", "main", "", false, true))
                    .getMessage().contains(target));
        }
        p.setProperty(target, "file:///done");
        assertEquals(VFSConfig.MOVE, result.equals("Process") ? new VFSConfig(p).getActionAfterProcess()
                : new VFSConfig(p).getActionAfterFailure());
    }

    @Test
    void bothFrameworkConstructorsValidateTheSequenceAndUriBeforePolling() {
        Properties p = properties();
        assertThrows(IllegalArgumentException.class, () -> new VFSConsumer(p, "files", null, 1000, " ", "", true, true));
        assertThrows(IllegalArgumentException.class, () -> new VFSConsumer(p, "files", null, "0/5 * * * * ?", null, "", true, true));
        p.remove(URI_KEY);
        assertTrue(assertThrows(IllegalArgumentException.class, () ->
                new VFSConsumer(p, "files", null, 1000, "main", "", true, false)).getMessage().contains(URI_KEY));
        p.setProperty(URI_KEY, "sftp:///missing-host/path");
        assertTrue(assertThrows(IllegalArgumentException.class, () ->
                new VFSConsumer(p, "files", null, "0/5 * * * * ?", "main", "", true, false)).getMessage().contains(URI_KEY));
        p.setProperty(URI_KEY, "file:///tmp");
        VFSConsumer interval = new VFSConsumer(p, "files", null, 1000, "main", "", true, false);
        VFSConsumer cron = new VFSConsumer(p, "files", null, "0/5 * * * * ?", "main", "", false, true);
        assertNull(interval.poll());
        cron.pause();
        cron.resume();
        assertNull(cron.poll());
        cron.destroy();
    }

    @Test
    void workersAreReadOnlyForParallelPolling() {
        Properties p = properties();
        p.setProperty("sequential", "true");
        p.setProperty(PREFIX + "WorkerPoolSize", "bad");
        assertEquals(4, new VFSConfig(p).getWorkerPoolSize());
    }

    @Test
    void checksLegacyAndExplicitStaleAges() {
        Properties p = properties();
        String old = PREFIX + "AutoLockReleaseInterval";
        p.setProperty(old, "1000");
        assertEquals(900000, new VFSConfig(p).getLockStaleAge());
        p.setProperty(PREFIX + "LockReleaseSameNode", "false");
        assertEquals(60000, new VFSConfig(p).getLockStaleAge());
        p.setProperty(old, "-1");
        assertEquals(-1, new VFSConfig(p).getLockStaleAge());
        p.setProperty(old, "bad");
        assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p));
        p.setProperty(PREFIX + "LockStaleAge", "12");
        assertEquals(60000, new VFSConfig(p).getLockStaleAge());
    }

    @Test
    void firstStoreConstructionOwnsTheLegacyWarnings(@TempDir Path folder) throws Exception {
        Properties p = properties();
        assertTrue(new VFSConfig(p).getFailStateParameters().deprecated.isEmpty());
        assertEquals("vfs-move-failed-records.properties", new VFSConfig(p).getOldList().getName());
        String[] keys = { PREFIX + "FailedRecordsFileName", PREFIX + "FailedRecordsFileDestination",
                PREFIX + "MoveFailedRecordTimestampFormat" };
        p.setProperty(keys[0], "legacy.txt");
        p.setProperty(keys[1], folder.toString() + java.io.File.separator);
        p.setProperty(keys[2], "unused");
        Path file = folder.resolve("legacy.txt");
        for (int bytes : new int[] { -1, 0, 3 }) {
            if (bytes >= 0) {
                Files.write(file, new byte[bytes]);
            }
            FailStateStore.Parameters params = new VFSConfig(p).getFailStateParameters();
            assertEquals(new HashSet<>(Arrays.asList(keys)), params.deprecated);
            assertEquals(file.toFile(), params.oldList);
            NamedLog log = mock(NamedLog.class);
            EndpointHealth health = mock(EndpointHealth.class);
            when(health.namedLog()).thenReturn(log);
            String endpoint = UUID.randomUUID().toString();
            FailStateStore first = FailStateStore.forEndpoint(endpoint, params, null, health);
            assertSame(first, FailStateStore.forEndpoint(endpoint, params, null, health));
            for (String key : keys) {
                verify(log).warn("The parameter " + key + " is deprecated.");
            }
            verify(log, times(bytes > 0 ? 1 : 0)).warn("The inbound no longer uses the old failed record list.");
            if (bytes < 0) {
                assertFalse(Files.exists(file));
            } else {
                assertArrayEquals(new byte[bytes], Files.readAllBytes(file));
            }
        }
    }

    @Test
    void mergesSourceAndTargetBeforeSettingEachOptionsObject() throws Exception {
        Properties p = properties();
        p.setProperty(URI_KEY, "sftp://src/in?" + AVOID + "=false");
        p.setProperty(TARGET, "sftp://dst/out?" + AVOID + "=false");
        VFSConfig cfg = new VFSConfig(p);
        assertFalse(cfg.isAvoidPermissionCheck());
        p.setProperty(AVOID, "true");
        cfg = new VFSConfig(p);
        SftpFileSystemConfigBuilder b = SftpFileSystemConfigBuilder.getInstance();
        FileSystemOptions source = Utils.attachFileSystemOptions(cfg.getOptions(URI_KEY), null);
        FileSystemOptions worker = Utils.attachFileSystemOptions(cfg.getOptions(URI_KEY), null);
        FileSystemOptions target = Utils.attachFileSystemOptions(cfg.getOptions(TARGET), null);
        assertEquals("true", b.getAvoidPermissionCheck(source));
        assertEquals("true", b.getAvoidPermissionCheck(worker));
        assertEquals("false", b.getAvoidPermissionCheck(target));
        b.setAvoidPermissionCheck(worker, "false");
        assertEquals("true", b.getAvoidPermissionCheck(source));
        assertNotSame(source, target);
        assertEquals("true", b.getAvoidPermissionCheck(Utils.attachFileSystemOptions(
                new VFSConfig(properties()).getOptions(URI_KEY), null)));
        Map<String, String> map = cfg.getOptions(URI_KEY);
        assertThrows(UnsupportedOperationException.class, () -> map.put(AVOID, "false"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SFTPSessionTimeout", "SFTPKeepAliveCount", "SFTPConnectTimeout", "FTPConnectTimeout",
            "FTPSocketTimeout", "FTPDataTimeout", "SMB2ReadTimeout", "SMB2WriteTimeout", "SMB2TransactTimeout",
            "WriteStallTimeout"})
    void validatesOptionalProtocolNumbers(String suffix) {
        String key = PREFIX + suffix;
        Properties p = properties();
        assertNull(new VFSConfig(p).getVfsSchemeProperties().get(key));
        p.setProperty(key, "45");
        assertEquals("45", new VFSConfig(p).getVfsSchemeProperties().get(key));
        p.setProperty(key, "bad");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p)).getMessage().contains(key));
        p.remove(key);
        p.setProperty(TARGET, "sftp://dst/out?" + key + "=-1");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p)).getMessage().contains(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SFTPSessionTimeout", "SFTPConnectTimeout", "FTPConnectTimeout", "FTPSocketTimeout"})
    void emptyTimeoutFieldHasTheSamePrecedenceAsAnOmittedField(String suffix) {
        String key = PREFIX + suffix;
        Properties p = properties();
        p.setProperty(key, "");
        assertNull(new VFSConfig(p).getOptions(URI_KEY).get(key));
        p.setProperty(URI_KEY, "sftp://src/in?" + key + "=42000");
        assertEquals("42000", new VFSConfig(p).getOptions(URI_KEY).get(key));
        p.setProperty(key, "30000");
        assertEquals("30000", new VFSConfig(p).getOptions(URI_KEY).get(key));
        p.setProperty(key, " ");
        assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p));
    }

    @Test
    void emptyUnrelatedNumericOptionStillFailsValidation() {
        Properties p = properties();
        p.setProperty(PREFIX + "FTPDataTimeout", "");
        assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p));
    }

    @Test
    void sshSettingsRemainLocalToEachOptionsObject() throws Exception {
        String[] keys = { "SFTPKeyExchangeAlgorithms", "SFTPHostKeyAlgorithms", "SFTPCiphers", "SFTPMacs",
                "SFTPPubkeyAcceptedAlgorithms" };
        Properties p = properties();
        for (String key : keys) {
            p.setProperty(PREFIX + key, key);
        }
        p.setProperty(TARGET, "sftp://dst/out?" + PREFIX + "SFTPCiphers=target");
        VFSConfig cfg = new VFSConfig(p);
        FileSystemOptions src = Utils.attachFileSystemOptions(cfg.getOptions(URI_KEY), null);
        FileSystemOptions dst = Utils.attachFileSystemOptions(cfg.getOptions(TARGET), null);
        SftpFileSystemConfigBuilder b = SftpFileSystemConfigBuilder.getInstance();
        assertArrayEquals(keys, new String[] { b.getKeyExchangeAlgorithm(src), b.getHostKeyAlgorithms(src),
                b.getCiphers(src), b.getMacs(src), b.getPubkeyAcceptedAlgorithms(src) });
        assertEquals("target", b.getCiphers(dst));
        assertEquals(keys[3], b.getMacs(dst));
    }

    @Test
    void decryptsEveryMatchWithOneReceiverCryptoAndKeepsInput() throws Exception {
        Properties p = properties();
        String first = "{wso2:vault-decrypt('first')}";
        String second = "{wso2:vault-decrypt('second')}";
        p.setProperty(PREFIX + "ContentType", first + second);
        p.setProperty(TARGET, "sftp://dst/" + first);
        Axis2SynapseEnvironment environment = mock(Axis2SynapseEnvironment.class, RETURNS_DEEP_STUBS);
        TransportInDescription receiver = new TransportInDescription("vfs");
        receiver.addParameter(new Parameter("keystore.identity.alias", "dummy"));
        when(environment.getAxis2ConfigurationContext().getAxisConfiguration().getTransportIn("vfs")).thenReturn(receiver);
        try (MockedConstruction<CryptoUtil> created = mockConstruction(CryptoUtil.class, (crypto, context) -> {
            assertEquals("dummy", ((Properties) context.arguments().get(0)).getProperty("keystore.identity.alias"));
            when(crypto.isInitialized()).thenReturn(true);
            when(crypto.decrypt(any())).thenAnswer(call -> {
                String token = new String((byte[]) call.getArgument(0), StandardCharsets.UTF_8);
                return ("first".equals(token) ? "$1" : "\\test").getBytes(StandardCharsets.UTF_8);
            });
        })) {
            VFSConfig cfg = new VFSConfig(p, environment, true);
            assertEquals("$1\\test", cfg.getContentType());
            assertEquals("sftp://dst/$1", cfg.getMoveAfterProcess());
            assertEquals(first + second, p.getProperty(PREFIX + "ContentType"));
            assertEquals(1, created.constructed().size());
        }
    }

    @Test
    void missingReceiverReportsKeyAndKeepsSecretsOutOfTheException() {
        Properties p = properties();
        p.setProperty(URI_KEY, "sftp://rig:{wso2:vault-decrypt('private-token')}@host/in");
        Throwable error = assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p));
        assertTrue(error.getMessage().contains(URI_KEY));
        assertTrue(error.getMessage().contains("vfs receiver keystore"));
        assertFalse(error.getMessage().contains("private-token"));
        assertNull(error.getCause());
    }

    @ParameterizedTest
    @CsvSource({"-,-,-,true,true", "false,-,-,false,false", "-,-,false,true,false",
            "-,false,-,false,false", "-,false,true,false,true", "false,true,-,true,true"})
    void appliesPermissionOptionsInOrder(String source, String endpoint, String target, String expectedSource,
                                        String expectedTarget) throws Exception {
        Properties p = properties();
        p.setProperty(URI_KEY, "sftp://source/in" + ("-".equals(source) ? "" : "?" + AVOID + "=" + source));
        p.setProperty(TARGET, "sftp://destination/out" + ("-".equals(target) ? "" : "?" + AVOID + "=" + target));
        if (!"-".equals(endpoint)) {
            p.setProperty(AVOID, endpoint);
        }
        VFSConfig config = new VFSConfig(p);
        SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
        assertEquals(expectedSource, builder.getAvoidPermissionCheck(Utils.attachFileSystemOptions(
                config.getOptions(URI_KEY), null)));
        assertEquals(expectedTarget, builder.getAvoidPermissionCheck(Utils.attachFileSystemOptions(
                config.getOptions(TARGET), null)));
    }

    @Test
    void inheritedListLocationDoesNotEmitADeprecatedName(@TempDir Path dir) {
        Properties defaults = properties();
        defaults.setProperty(PREFIX + "FailedRecordsFileDestination", dir.toString() + java.io.File.separator);
        VFSConfig config = new VFSConfig(new Properties(defaults));
        assertTrue(config.getFailStateParameters().deprecated.isEmpty());
        assertEquals(dir.resolve("vfs-move-failed-records.properties").toFile(), config.getOldList());
    }

    @Test
    void readsLocalDriveAndUnderscoredHostWithoutLookingThemUp() {
        Properties p = properties();
        for (String source : new String[] { "C:/incoming", "sftp://file_server/in" }) {
            p.setProperty(URI_KEY, source);
            assertEquals(source, new VFSConfig(p).getFileURI());
        }
        p.setProperty(URI_KEY, "sftp://host:65536/in");
        assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p));
    }

    @ParameterizedTest
    @ValueSource(strings = { "ftp", "ftps" })
    void ftpTransferTypeIsCaseInsensitive(String scheme) throws Exception {
        Properties p = properties();
        p.setProperty(URI_KEY, scheme + "://host/in?filetype=ascii");
        FileSystemOptions opts = Utils.attachFileSystemOptions(new VFSConfig(p).getOptions(URI_KEY), null);
        FtpFileSystemConfigBuilder ftp = scheme.equals("ftp")
                ? FtpFileSystemConfigBuilder.getInstance()
                : FtpsFileSystemConfigBuilder.getInstance();
        assertEquals("ASCII", ftp.getFileType(opts).name());
    }

    @ParameterizedTest
    @CsvSource({"org.apache.commons.net.ftp.FTPClient,commons-net-3.9.0.wso2v1.jar",
            "com.jcraft.jsch.JSch,jsch-2.27.7.wso2v1.jar", "com.hierynomus.smbj.SMBClient,smbj-0.14.0.wso2v2.jar",
            "com.hierynomus.asn1.types.ASN1Object,smbj-0.14.0.wso2v2.jar"})
    void loadsPackLibrariesForTheUnitSuite(String className, String jar) throws Exception {
        java.net.URL location = Class.forName(className).getProtectionDomain().getCodeSource().getLocation();
        assertEquals(jar, java.nio.file.Paths.get(location.toURI()).getFileName().toString());
    }

    @Test
    void invalidFtpTransferTypeNamesTheOptionBeforePolling() {
        Properties p = properties();
        p.setProperty(TARGET, "ftps://destination/out?filetype=invalid");
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p));
        assertEquals("Invalid value for filetype.", failure.getMessage());
    }

    @Test
    void diagnosticsAreScopedToTheConstructingEndpointAndEmittedOnce() {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(VFSConfig.class.getName());
        java.util.List<String> warnings = new java.util.ArrayList<>();
        java.util.logging.Handler handler = new java.util.logging.Handler() {
            public void publish(java.util.logging.LogRecord record) {
                if (record.getLevel() == java.util.logging.Level.WARNING) {
                    warnings.add(record.getMessage());
                }
            }
            public void flush() { }
            public void close() { }
        };
        logger.addHandler(handler);
        try {
            Properties p = properties();
            VFSConsumer control = new VFSConsumer(p, "noWarnings", null, 5000, "main", null, false, true);
            assertTrue(warnings.isEmpty());
            String[] ignored = {"ResolveHostsDynamically", "waitTimeBeforeRead", "DistributedLock",
                    "DistributedTimeout", "ActionAfterErrors", "MoveAfterErrors", "ClusterAware"};
            for (String key : ignored) {
                p.setProperty(PREFIX + key, "ignored-value");
            }
            p.setProperty(PREFIX + "ReconnectTimeout", "1000");
            VFSConsumer endpoint = new VFSConsumer(p, "H4Bare", null, 5000, "main", null, false, true);
            endpoint.poll();
            endpoint.pause();
            endpoint.resume();
            endpoint.destroy();
            control.destroy();
            assertEquals(ignored.length + 1, warnings.size());
            for (String key : ignored) {
                assertEquals(1, warnings.stream().filter(w -> w.startsWith("[inbound:H4Bare] ")
                        && w.contains(PREFIX + key + " is ignored.")).count(), key);
            }
            assertTrue(warnings.stream().anyMatch(w -> w.contains("1000 ms") && w.contains("unit")));
            assertTrue(warnings.stream().anyMatch(w -> w.contains("MinimumAge")));
            assertTrue(warnings.stream().anyMatch(w -> w.contains("Every new connection already resolves")));
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void looksOnlyAtTheActualLegacyDestination(@TempDir Path folder) throws Exception {
        Properties p = properties();
        assertEquals(new java.io.File("repository/conf/vfs-move-failed-records.properties"),
                new VFSConfig(p).getOldList());
        assertLegacyFileWarning(p, false);

        String filename = PREFIX + "FailedRecordsFileName";
        String destination = PREFIX + "FailedRecordsFileDestination";
        Path actual = folder.resolve("prefixlist");
        Path directory = Files.createDirectory(folder.resolve("prefix"));
        Path joined = directory.resolve("list");
        Files.write(joined, new byte[] { 1 });
        p.setProperty(filename, "list");
        p.setProperty(destination, directory.toString());
        assertEquals(actual.toFile(), new VFSConfig(p).getOldList());
        assertLegacyFileWarning(p, false);
        assertFalse(Files.exists(actual));
        assertArrayEquals(new byte[] { 1 }, Files.readAllBytes(joined));
        Files.write(actual, new byte[] { 2 });
        assertLegacyFileWarning(p, true);
        assertArrayEquals(new byte[] { 2 }, Files.readAllBytes(actual));

        p.setProperty(destination, directory.toString() + java.io.File.separator);
        assertEquals(joined.toFile(), new VFSConfig(p).getOldList());
        assertLegacyFileWarning(p, true);
        p.setProperty(destination, "");
        p.setProperty(filename, java.nio.file.Paths.get("").toAbsolutePath().relativize(joined).toString());
        assertFalse(new VFSConfig(p).getOldList().isAbsolute());
        assertEquals(joined.toRealPath(), new VFSConfig(p).getOldList().toPath().toRealPath());
        assertLegacyFileWarning(p, true);
    }

    private static void assertLegacyFileWarning(Properties properties, boolean expected) {
        FailStateStore.Parameters params = new VFSConfig(properties).getFailStateParameters();
        NamedLog log = mock(NamedLog.class);
        EndpointHealth health = mock(EndpointHealth.class);
        when(health.namedLog()).thenReturn(log);
        FailStateStore.forEndpoint(UUID.randomUUID().toString(), params, null, health);
        verify(log, times(expected ? 1 : 0)).warn("The inbound no longer uses the old failed record list.");
    }

    @Test
    void sourceAndTargetHaveTheirOwnIdentityAndUserDirectory(@TempDir Path folder) throws Exception {
        Path first = Files.createFile(folder.resolve("first.key"));
        Path second = Files.createFile(folder.resolve("second.key"));
        Properties p = properties();
        p.setProperty(PREFIX + "SFTPIdentities", first.toString());
        p.setProperty(PREFIX + "SFTPIdentityPassPhrase", "fixture-phrase");
        p.setProperty(PREFIX + "SFTPUserDirIsRoot", "true");
        p.setProperty(TARGET, "sftp://dst/out?" + PREFIX + "SFTPIdentities=" + second
                + "&" + PREFIX + "SFTPUserDirIsRoot=false");
        VFSConfig c = new VFSConfig(p);
        DefaultFileSystemManager manager = new DefaultFileSystemManager();
        try {
            manager.addProvider("sftp", new SftpFileProvider());
            manager.init();
            FileSystemOptions src = Utils.attachFileSystemOptions(c.getOptions(URI_KEY), manager);
            FileSystemOptions target = Utils.attachFileSystemOptions(c.getOptions(TARGET), manager);
            SftpFileSystemConfigBuilder b = SftpFileSystemConfigBuilder.getInstance();
            assertEquals(first.toFile(), b.getIdentities(src)[0]);
            assertEquals(second.toFile(), b.getIdentities(target)[0]);
            assertEquals("fixture-phrase", b.getIdentityPassPhrase(target));
            assertEquals(Boolean.TRUE, b.getUserDirIsRoot(src));
            assertEquals(Boolean.FALSE, b.getUserDirIsRoot(target));
        } finally {
            manager.close();
        }
    }

    @Test
    void setsOptionalBoundsOnTheirActualProtocolBuilders() throws Exception {
        Properties p = properties();
        p.setProperty(PREFIX + "SFTPSessionTimeout", "123");
        p.setProperty(PREFIX + "SFTPConnectTimeout", "456");
        p.setProperty(PREFIX + "SFTPKeepAliveCount", "7");
        p.setProperty(PREFIX + "WriteStallTimeout", "0");
        SftpFileSystemConfigBuilder sftp = SftpFileSystemConfigBuilder.getInstance();
        FileSystemOptions src = Utils.attachFileSystemOptions(new VFSConfig(p).getOptions(URI_KEY), null);
        assertEquals(123, sftp.getSessionTimeout(src).toMillis());
        assertEquals(456, sftp.getConnectTimeout(src).toMillis());
        assertEquals(7, sftp.getKeepAliveCountMax(src));
        assertEquals(0, sftp.getWriteStallTimeout(src).toMillis());
        p.setProperty(TARGET, "ftps://other/out?vfs.implicit=true&vfs.passive=true&vfs.protection=P");
        p.setProperty(PREFIX + "FTPConnectTimeout", "108");
        p.setProperty(PREFIX + "FTPSocketTimeout", "109");
        p.setProperty(PREFIX + "FTPDataTimeout", "110");
        FileSystemOptions target = Utils.attachFileSystemOptions(new VFSConfig(p).getOptions(TARGET), null);
        FtpsFileSystemConfigBuilder ftps = FtpsFileSystemConfigBuilder.getInstance();
        assertEquals(108, ftps.getConnectTimeout(target));
        assertEquals(109, ftps.getSoTimeoutDuration(target).toMillis());
        assertEquals(110, ftps.getDataTimeoutDuration(target).toMillis());
        assertEquals("IMPLICIT", ftps.getFtpsMode(target).name());
        assertEquals("P", ftps.getDataChannelProtectionLevel(target).name());
        assertTrue(ftps.getPassiveMode(target));
        p.setProperty(TARGET, "smb2://other/share/out?vfs.EncryptionEnabled=true");
        p.setProperty(PREFIX + "SMB2ReadTimeout", "21");
        p.setProperty(PREFIX + "SMB2WriteTimeout", "22");
        p.setProperty(PREFIX + "SMB2TransactTimeout", "23");
        target = Utils.attachFileSystemOptions(new VFSConfig(p).getOptions(TARGET), null);
        Smb2FileSystemConfigBuilder smb = Smb2FileSystemConfigBuilder.getInstance();
        assertEquals(21, smb.getReadTimeout(target).toMillis());
        assertEquals(22, smb.getWriteTimeout(target).toMillis());
        assertEquals(23, smb.getTransactTimeout(target).toMillis());
        assertTrue(smb.getEncryptionEnabled(target));
    }

    @Test
    void moveFormatIsValidatedOnceAndReturnedWithIndependentState() {
        Properties p = properties();
        p.setProperty(PREFIX + "MoveTimestampFormat", "yyyyMMdd");
        VFSConfig config = new VFSConfig(p);
        java.text.SimpleDateFormat first = (java.text.SimpleDateFormat) config.getMoveTimestampFormat();
        first.applyPattern("ss");
        assertEquals("yyyyMMdd", config.getMoveTimestampPattern());
        assertEquals("yyyyMMdd", ((java.text.SimpleDateFormat) config.getMoveTimestampFormat()).toPattern());
    }

    @Test
    void mountedHintUsesTheUriAndEndpointIndependentlyOfLocking() {
        Properties p = properties();
        String key = PREFIX + "IsMounted";
        assertNull(new VFSConfig(p).getOptions(URI_KEY).get(key));
        p.setProperty(URI_KEY, "file:///source?" + key + "=false");
        p.setProperty(key, "true");
        p.setProperty(TARGET, "file:///target?" + key + "=false");
        VFSConfig config = new VFSConfig(p);
        assertEquals("true", config.getOptions(URI_KEY).get(key));
        assertEquals("false", config.getOptions(TARGET).get(key));
        assertTrue(config.isFileLocking());
        p.setProperty(key, "invalid");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p)).getMessage().contains(key));
    }

    @Test
    void protocolStoreValuesAndAccessMasksReachTheSelectedBuilder() throws Exception {
        Properties p = properties();
        p.setProperty(TARGET, "ftps://other/out");
        for (String name : new String[] { "keystore", "truststore", "kspassword", "tspassword", "keypassword" }) {
            String key = "vfs.ssl." + name;
            assertNull(new VFSConfig(p).getOptions(TARGET).get(key));
            p.setProperty(key, "fixture-" + name);
        }
        FtpsFileSystemConfigBuilder ftps = FtpsFileSystemConfigBuilder.getInstance();
        FileSystemOptions options = Utils.attachFileSystemOptions(new VFSConfig(p).getOptions(TARGET), null);
        assertEquals("fixture-keystore", ftps.getKeyStore(options));
        assertEquals("fixture-truststore", ftps.getTrustStore(options));
        assertEquals("fixture-kspassword", ftps.getKeyStorePW(options));
        assertEquals("fixture-tspassword", ftps.getTrustStorePW(options));
        assertEquals("fixture-keypassword", ftps.getKeyPW(options));
        p.setProperty(TARGET, "smb2://other/share/out");
        Smb2FileSystemConfigBuilder smb = Smb2FileSystemConfigBuilder.getInstance();
        for (String mask : new String[] { "GENERIC_READ", "invalid-mask" }) {
            p.setProperty("vfs.diskShareAccessMask", mask);
            options = Utils.attachFileSystemOptions(new VFSConfig(p).getOptions(TARGET), null);
            assertEquals(Arrays.asList(mask.equals("GENERIC_READ") ? mask : "MAXIMUM_ALLOWED"),
                    smb.getDiskShareAccessMask(options));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "transport.vfs.FileURI", "transport.vfs.MoveAfterProcess" })
    void acceptsProviderFileNamesWithoutChangingTheirEscapes(String key, @TempDir Path dir) throws Exception {
        Files.write(dir.resolve("invoice[1].csv"), new byte[] { 17 });
        Files.write(dir.resolve("invoice%5B1%5D.csv"), new byte[] { 42 });
        DefaultFileSystemManager manager = new DefaultFileSystemManager();
        manager.addProvider("file", new DefaultLocalFileProvider());
        manager.addProvider("sftp", new SftpFileProvider());
        manager.init();
        try {
            for (String spelling : new String[] { "invoice[1].csv", "invoice%5B1%5D.csv", "invoice%255B1%255D.csv" }) {
                String local = "file://" + dir + "/" + spelling;
                String remote = "sftp://files.example/" + spelling;
                for (String uri : new String[] { local, remote, dir + "/" + spelling }) {
                    Properties properties = properties();
                    properties.setProperty(key, uri);
                    VFSConfig config = new VFSConfig(properties);
                    String selected = URI_KEY.equals(key) ? config.getFileURI() : config.getMoveAfterProcess();
                    assertEquals(uri, selected);
                    if (uri.equals(local)) {
                        try (FileObject file = manager.resolveFile(selected);
                             java.io.InputStream body = file.getContent().getInputStream()) {
                            assertEquals(spelling.contains("%25") ? 42 : 17, body.read());
                        }
                    } else if (uri.equals(remote)) {
                        // Resolve the provider name. This does not open an SSH session.
                        assertEquals(manager.resolveURI(local).getBaseName(), manager.resolveURI(selected).getBaseName());
                    }
                }
            }
        } finally {
            manager.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "sftp://first last@server_name:22/invoice[1].csv",
            "sftp://[::1]:22/invoice[1].csv", "sftp://server", "vfs:sftp://server/in/*?key=value",
            "file:///", "file:/in/with space/[part].csv", "C:/in/[part].csv" })
    void preservesValidLocationsAndQueryOptions(String location) {
        Properties p = properties();
        p.setProperty(URI_KEY, location);
        VFSConfig config = new VFSConfig(p);
        assertEquals(location, config.getFileURI());
        if (location.contains("?key=")) {
            assertEquals("value", config.getOptions(URI_KEY).get("key"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "sftp://user:a^b{c}d|e\"f<g>h`i@host/in", "ftp://user:a#b@host:21/in",
            "smb2://us^er:a}b@host/share/in" })
    void acceptsAUserAndPasswordWithCharactersThatAUriRefuses(String location) {
        Properties p = properties();
        p.setProperty(URI_KEY, location);
        assertEquals(location, new VFSConfig(p).getFileURI());
    }

    @ParameterizedTest
    @ValueSource(strings = { "transport.vfs.FileURI", "transport.vfs.MoveAfterProcess",
            "transport.vfs.MoveAfterFailure", "transport.vfs.MoveAfterFailedMove", "transport.vfs.ReplyFileURI" })
    void invalidUriNamesOnlyItsParameter(String key) {
        for (String invalid : new String[] { "?", "?key=private", "vfs:?", "vfs:", "file:",
                "sftp:///in", "sftp://u:private@/in", "sftp://host:bogus/in", "sftp://host:/in",
                "sftp://host:65536/in", "sftp://[::1/in", "sftp://host/%xx", "file:///in/%",
                "sftp://host/[name]?key=%0" }) {
            Properties p = properties();
            p.setProperty(key, invalid);
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> new VFSConfig(p));
            assertEquals("Invalid value for " + key + ".", error.getMessage());
            assertNull(error.getCause());
        }
    }
}
