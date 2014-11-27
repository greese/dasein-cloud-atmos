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

package org.dasein.cloud.atmos;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.dc.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Fakes the concept of data center services for standalone Atmos implementations.
 * <p>Created by George Reese: 10/8/12 10:42 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class AtmosGeography extends AbstractDataCenterServices<Atmos> {
    private Atmos provider;

    AtmosGeography(@Nonnull Atmos provider) { super(provider); }

    private volatile transient GeographyCapabilities capabilities;

    @Override
    public @Nonnull DataCenterCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new GeographyCapabilities(getProvider());
        }
        return capabilities;
    }

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
        List<DataCenter> dataCenters= new ArrayList<DataCenter>();

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
            List<Region> tmp = new ArrayList<Region>();
            Region region = new Region();

            if( provider.getAtmosProvider().equals(AtmosProvider.ATT) ) {
                region.setActive(true);
                region.setAvailable(true);
                region.setName("US 1");
                region.setProviderRegionId("us");
                region.setJurisdiction("US");
                tmp.add(region);
            }
            else {
                region.setActive(true);
                region.setAvailable(true);
                region.setName("Region 1");
                region.setProviderRegionId("r1");
                region.setJurisdiction("US");
                tmp.add(region);
            }
            regions = tmp;
        }
        return regions;
    }
}
