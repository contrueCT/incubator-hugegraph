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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.inject.Provider;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.UriInfo;

public class LoadDetectFilterTest extends BaseUnitTest {

    private LoadDetectFilter loadDetectFilter;
    private ContainerRequestContext requestContext;
    private UriInfo uriInfo;
    private WorkLoad workLoad;

    @Before
    public void setup() {
        this.requestContext = Mockito.mock(ContainerRequestContext.class);
        this.uriInfo = Mockito.mock(UriInfo.class);
        this.workLoad = new WorkLoad();

        Mockito.when(this.requestContext.getUriInfo()).thenReturn(this.uriInfo);
        Mockito.when(this.requestContext.getMethod()).thenReturn("GET");

        this.loadDetectFilter = new LoadDetectFilter();
        this.setLoadProvider(this.workLoad);
        this.setConfigProvider(createConfig(8, 0));
    }

    @Test
    public void testFilter_WhiteListPathIgnored() {
        setupPath("", List.of(""));
        this.workLoad.incrementAndGet();

        this.loadDetectFilter.filter(this.requestContext);

        Assert.assertEquals(1, this.workLoad.get().get());
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
    }

    @Test
    public void testFilter_RejectsWhenFreeMemoryIsTooLow() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, Integer.MAX_VALUE));

        ServiceUnavailableException exception = (ServiceUnavailableException) Assert.assertThrows(
                ServiceUnavailableException.class,
                () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertContains("The server available memory",
                              exception.getMessage());
        Assert.assertContains(ServerOptions.MIN_FREE_MEMORY.name(),
                              exception.getMessage());
    }

    @Test
    public void testFilter_AllowsRequestWhenLoadAndMemoryAreHealthy() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, 0));

        this.loadDetectFilter.filter(this.requestContext);

        Assert.assertEquals(1, this.workLoad.get().get());
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
}
