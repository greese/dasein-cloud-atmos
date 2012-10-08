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

import junit.framework.Test;
import org.dasein.cloud.test.ComprehensiveTestSuite;
import org.dasein.cloud.test.TestConfigurationException;

/**
 * Bootstraps the Dasein Cloud test stuff for the Atmos implementation.
 * <p>Created by George Reese: 10/8/12 11:05 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.02
 */
public class AtmosTestSuite   {
    static public Test suite() throws TestConfigurationException {
        return new ComprehensiveTestSuite(Atmos.class);
    }
}
