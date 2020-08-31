/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.makeUpdateListener;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * Thin client distributed configuration.
 */
public class DistibutedThinClientConfiguration {
    /** */
    private final IgniteLogger log;

    /** . */
    private final DistributedChangeableProperty<Boolean> showStackTrace =
        detachedBooleanProperty("thinClientProperty.showStackTrace");

    /** Message of baseline auto-adjust parameter was changed. */
    private static final String PROPERTY_UPDATE_MESSAGE =
        "ThinClientProperty parameter '%s' was changed from '%s' to '%s'";

    /**
     * @param ctx Kernal context.
     */
    public DistibutedThinClientConfiguration(
        GridKernalContext ctx
    ) {
        log = ctx.log(DistibutedThinClientConfiguration.class);

        GridInternalSubscriptionProcessor isp = ctx.internalSubscriptionProcessor();

        isp.registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    showStackTrace.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    dispatcher.registerProperties(showStackTrace);
                }
            }
        );
    }

    /**
     * @param showStack If {@code true} shows full stack trace on the client side.
     */
    public GridFutureAdapter<?> updateThinClientShowStackTraceAsync(boolean showStack) throws IgniteCheckedException {
        return showStackTrace.propagateAsync(showStack);
    }

    /**
     * @return If {@code true} shows full stack trace on the client side.
     */
    @Nullable public Boolean showFullStack() {
        return showStackTrace.get();
    }
}
