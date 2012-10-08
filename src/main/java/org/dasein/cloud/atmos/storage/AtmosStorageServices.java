/**
 * ========= CONFIDENTIAL =========
 *
 * Copyright (C) 2012 enStratus Networks Inc - ALL RIGHTS RESERVED
 *
 * ====================================================================
 *  NOTICE: All information contained herein is, and remains the
 *  property of enStratus Networks Inc. The intellectual and technical
 *  concepts contained herein are proprietary to enStratus Networks Inc
 *  and may be covered by U.S. and Foreign Patents, patents in process,
 *  and are protected by trade secret or copyright law. Dissemination
 *  of this information or reproduction of this material is strictly
 *  forbidden unless prior written permission is obtained from
 *  enStratus Networks Inc.
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
    public @Nonnull AtmosObjectStore getBlobStoreSupport() {
        return new AtmosObjectStore(provider);
    }
}
