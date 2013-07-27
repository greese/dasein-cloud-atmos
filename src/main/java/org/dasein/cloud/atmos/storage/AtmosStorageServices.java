/**
 * Copyright (C) 2012-2013 Dell, Inc.
 * See annotations for authorship information
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.atmos.storage;

import org.dasein.cloud.atmos.Atmos;
import org.dasein.cloud.storage.AbstractStorageServices;

import javax.annotation.Nonnull;

/**
 * Implements the Dasein Cloud storage services API for the EMC Atmos.
 * <p>Created by George Reese: 10/5/12 8:51 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class AtmosStorageServices extends AbstractStorageServices {
    private Atmos provider;

    public AtmosStorageServices(Atmos provider) { this.provider = provider; }

    @Override
    public @Nonnull AtmosObjectStore getOnlineStorageSupport() {
        return new AtmosObjectStore(provider);
    }
}
