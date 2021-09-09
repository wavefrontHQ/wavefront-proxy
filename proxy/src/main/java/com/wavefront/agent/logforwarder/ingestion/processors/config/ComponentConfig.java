/*
 * Copyright (c) 2019 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.wavefront.agent.logforwarder.ingestion.processors.config;


import com.wavefront.agent.logforwarder.ingestion.processors.Processor;

import java.util.List;

public class ComponentConfig {

    /* component name */
    public String component;

    /* syslog port */
    public int syslogPort;

    /* rest api port */
    public int httpPort;

    /* list of processors */
    public List<Processor> processors;

    /* In memory buffer when the consumer is slow */
    public int bufferSize;

    @Override
    public String toString() {
        return "ComponentConfig{" + "component='" + component + '\'' + ", syslogPort=" + syslogPort
                + ", processors=" + processors + '}';
    }
}
