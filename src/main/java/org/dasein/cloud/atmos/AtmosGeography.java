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
package org.dasein.cloud.atmos;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

/**
 * Fakes the concept of data center services for standalone Atmos implementations.
 * <p>Created by George Reese: 10/8/12 10:42 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class AtmosGeography implements DataCenterServices {
    private Atmos provider;

    AtmosGeography(@Nonnull Atmos provider) { this.provider = provider; }

    @Override
    public @Nullable DataCenter getDataCenter(@Nonnull String dataCenterId) throws InternalException, CloudException {
        if( provider.getAtmosProvider().equals(AtmosProvider.ATT) ) {
            if( dataCenterId.equals("us1") ) {
                DataCenter dc = new DataCenter();

                dc.setActive(true);
                dc.setAvailable(true);
                dc.setName("US 1");
                dc.setProviderDataCenterId("us1");
                dc.setRegionId("us");
                return dc;
            }
        }
        else {
            if( dataCenterId.equals("dc1") ) {
                DataCenter dc = new DataCenter();

                dc.setActive(true);
                dc.setAvailable(true);
                dc.setName("DC 1");
                dc.setProviderDataCenterId("dc1");
                dc.setRegionId("r1");
                return dc;
            }
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "data center";
    }

    @Override
    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "region";
    }

    @Override
    public @Nullable Region getRegion(@Nonnull String providerRegionId) throws InternalException, CloudException {
        for( Region r : listRegions() ) {
            if( providerRegionId.equals(r.getProviderRegionId()) ) {
                return r;
            }
        }
        return null;
    }

    @Override
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String providerRegionId) throws InternalException, CloudException {
        Region region = getRegion(providerRegionId);

        if( region == null ) {
            throw new CloudException("No such region: " + providerRegionId);
        }
        ArrayList<DataCenter> dataCenters= new ArrayList<DataCenter>();

        if( providerRegionId.equals("us") && provider.getAtmosProvider().equals(AtmosProvider.ATT) ) {
            DataCenter dc = new DataCenter();

            dc.setActive(true);
            dc.setAvailable(true);
            dc.setName("US 1");
            dc.setProviderDataCenterId("us1");
            dc.setRegionId("us");
            dataCenters.add(dc);
        }
        else if( providerRegionId.equals("r1") && !provider.getAtmosProvider().equals(AtmosProvider.ATT) ) {
            DataCenter dc = new DataCenter();

            dc.setActive(true);
            dc.setAvailable(true);
            dc.setName("Region 1/DC 1");
            dc.setProviderDataCenterId("dc1");
            dc.setRegionId("r1");
            dataCenters.add(dc);
        }
        return dataCenters;
    }

    private transient Collection<Region> regions;

    @Override
    public Collection<Region> listRegions() throws InternalException, CloudException {
        if( regions == null ) {
            ArrayList<Region> tmp = new ArrayList<Region>();
            Region region = new Region();

            if( provider.getAtmosProvider().equals(AtmosProvider.ATT) ) {
                region.setActive(true);
                region.setAvailable(true);
                region.setName("US 1");
                region.setProviderRegionId("us");
                tmp.add(region);
            }
            else {
                region.setActive(true);
                region.setAvailable(true);
                region.setName("Region 1");
                region.setProviderRegionId("r1");
                tmp.add(region);
            }
            regions = tmp;
        }
        return regions;
    }
}
