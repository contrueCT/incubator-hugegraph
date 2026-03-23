/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.unit.api.filter;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.api.filter.LoadDetectFilter;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.define.WorkLoad;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.inject.Provider;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.UriInfo;

public class LoadDetectFilterTest extends BaseUnitTest {

    private static final Logger TEST_LOGGER =
            (Logger) LogManager.getLogger(LoadDetectFilter.class);

    private LoadDetectFilter loadDetectFilter;
    private ContainerRequestContext requestContext;
    private UriInfo uriInfo;
    private WorkLoad workLoad;
    private TestAppender testAppender;

    @Before
    public void setup() {
        this.requestContext = Mockito.mock(ContainerRequestContext.class);
        this.uriInfo = Mockito.mock(UriInfo.class);
        this.workLoad = new WorkLoad();
        this.testAppender = new TestAppender();
        this.testAppender.start();
        TEST_LOGGER.addAppender(this.testAppender);

        Mockito.when(this.requestContext.getUriInfo()).thenReturn(this.uriInfo);
        Mockito.when(this.requestContext.getMethod()).thenReturn("GET");

        this.loadDetectFilter = new TestLoadDetectFilter();
        this.setLoadProvider(this.workLoad);
        this.setConfigProvider(createConfig(8, 0));
    }

    @After
    public void teardown() {
        TEST_LOGGER.removeAppender(this.testAppender);
        this.testAppender.stop();
    }

    @Test
    public void testFilter_WhiteListPathIgnored() {
        setupPath("", List.of(""));
        this.workLoad.incrementAndGet();

        this.loadDetectFilter.filter(this.requestContext);

        Assert.assertEquals(1, this.workLoad.get().get());
        Assert.assertTrue(this.testAppender.events().isEmpty());
    }

    @Test
    public void testFilter_RejectsWhenWorkerLoadIsTooHigh() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(2, 0));
        this.workLoad.incrementAndGet();

        ServiceUnavailableException exception = (ServiceUnavailableException) Assert.assertThrows(
                ServiceUnavailableException.class,
                () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertContains("The server is too busy to process the request",
                              exception.getMessage());
        Assert.assertContains(ServerOptions.MAX_WORKER_THREADS.name(),
                              exception.getMessage());
        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Rejected request due to high worker load");
        this.assertWarnLogContains("method=GET");
        this.assertWarnLogContains("path=graphs/hugegraph/vertices");
        this.assertWarnLogContains("currentLoad=2");
    }

    @Test
    public void testFilter_RejectsWhenFreeMemoryIsTooLow() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, Integer.MAX_VALUE));
        this.setGcTriggered(false);

        ServiceUnavailableException exception = (ServiceUnavailableException) Assert.assertThrows(
                ServiceUnavailableException.class,
                () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertContains("The server available memory",
                              exception.getMessage());
        Assert.assertContains(ServerOptions.MIN_FREE_MEMORY.name(),
                              exception.getMessage());
        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Rejected request due to low free memory");
        this.assertWarnLogContains("method=GET");
        this.assertWarnLogContains("path=graphs/hugegraph/vertices");
        this.assertWarnLogContains("gcTriggered=false");
    }

    @Test
    public void testFilter_AllowsRequestWhenLoadAndMemoryAreHealthy() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, 0));

        this.loadDetectFilter.filter(this.requestContext);

        Assert.assertEquals(1, this.workLoad.get().get());
        Assert.assertTrue(this.testAppender.events().isEmpty());
    }

    @Test
    public void testFilter_RejectLogIsRateLimited() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(2, 0));
        this.setAllowRejectLogs(true, false);

        this.workLoad.incrementAndGet();
        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> this.loadDetectFilter.filter(this.requestContext));

        this.workLoad.get().set(1);
        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Rejected request due to high worker load");
    }

    private HugeConfig createConfig(int maxWorkerThreads, int minFreeMemory) {
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(ServerOptions.MAX_WORKER_THREADS.name(), maxWorkerThreads);
        conf.setProperty(ServerOptions.MIN_FREE_MEMORY.name(), minFreeMemory);
        return new HugeConfig(conf);
    }

    private void setupPath(String path, List<String> segments) {
        List<PathSegment> pathSegments = segments.stream()
                                                 .map(this::createPathSegment)
                                                 .collect(Collectors.toList());
        Mockito.when(this.uriInfo.getPath()).thenReturn(path);
        Mockito.when(this.uriInfo.getPathSegments()).thenReturn(pathSegments);
    }

    private PathSegment createPathSegment(String path) {
        PathSegment segment = Mockito.mock(PathSegment.class);
        Mockito.when(segment.getPath()).thenReturn(path);
        return segment;
    }

    private void setLoadProvider(WorkLoad workLoad) {
        Whitebox.setInternalState(this.loadDetectFilter, "loadProvider",
                                  (Provider<WorkLoad>) () -> workLoad);
    }

    private void setConfigProvider(HugeConfig config) {
        Whitebox.setInternalState(this.loadDetectFilter, "configProvider",
                                  (Provider<HugeConfig>) () -> config);
    }

    private void setGcTriggered(boolean gcTriggered) {
        ((TestLoadDetectFilter) this.loadDetectFilter).gcTriggered(gcTriggered);
    }

    private void setAllowRejectLogs(boolean... allowedLogs) {
        ((TestLoadDetectFilter) this.loadDetectFilter).allowRejectLogs(allowedLogs);
    }

    private void assertWarnLogContains(String expectedContent) {
        Assert.assertFalse(this.testAppender.events().isEmpty());
        LogEvent event = this.testAppender.events().get(0);
        Assert.assertEquals(Level.WARN, event.getLevel());
        Assert.assertContains(expectedContent,
                              event.getMessage().getFormattedMessage());
    }

    private static class TestLoadDetectFilter extends LoadDetectFilter {

        private boolean gcTriggered;
        private final Deque<Boolean> allowRejectLogs = new ArrayDeque<>();

        public void gcTriggered(boolean gcTriggered) {
            this.gcTriggered = gcTriggered;
        }

        public void allowRejectLogs(boolean... allowedLogs) {
            this.allowRejectLogs.clear();
            for (boolean allowedLog : allowedLogs) {
                this.allowRejectLogs.addLast(allowedLog);
            }
        }

        @Override
        protected boolean gcIfNeeded() {
            return this.gcTriggered;
        }

        @Override
        protected boolean allowRejectLog() {
            if (this.allowRejectLogs.isEmpty()) {
                return true;
            }
            return this.allowRejectLogs.removeFirst();
        }
    }

    private static class TestAppender extends AbstractAppender {

        private final List<LogEvent> events;

        protected TestAppender() {
            super("LoadDetectFilterTestAppender", (Filter) null,
                  (Layout<? extends Serializable>) null, false,
                  Property.EMPTY_ARRAY);
            this.events = new ArrayList<>();
        }

        @Override
        public void append(LogEvent event) {
            this.events.add(event.toImmutable());
        }

        public List<LogEvent> events() {
            return this.events;
        }
    }
}
