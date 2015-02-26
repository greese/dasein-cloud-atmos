package org.dasein.cloud.atmos;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.dc.DataCenterCapabilities;

import java.util.Locale;

/**
 * Created by stas on 27/11/2014.
 */
public class GeographyCapabilities extends AbstractCapabilities<Atmos> implements DataCenterCapabilities {
    public GeographyCapabilities( Atmos provider ) {
        super(provider);
    }

    @Override
    public String getProviderTermForDataCenter( Locale locale ) {
        return "data center";
    }

    @Override
    public String getProviderTermForRegion( Locale locale ) {
        return "region";
    }

    @Override
    public boolean supportsAffinityGroups() {
        return false;
    }

    @Override
    public boolean supportsResourcePools() {
        return false;
    }

    @Override
    public boolean supportsStoragePools() {
        return false;
    }

    @Override
    public boolean supportsFolders() {
        return false;
    }
}
