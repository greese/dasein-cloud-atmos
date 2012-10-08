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

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.NameRules;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.atmos.Atmos;
import org.dasein.cloud.atmos.AtmosMethod;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.storage.AbstractBlobStoreSupport;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.util.uom.storage.*;
import org.dasein.util.uom.storage.Byte;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Locale;

/**
 * Provides interaction with the EMC Atmos object storage engine in accordance with the Dasein Cloud API.
 * <p>Created by George Reese: 10/5/12 8:52 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class AtmosObjectStore extends AbstractBlobStoreSupport {
    private Atmos provider;

    AtmosObjectStore(Atmos provider) { this.provider = provider; }


    @Override
    public boolean allowsNestedBuckets() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean allowsRootObjects() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsPublicSharing() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull Blob createBucket(@Nonnull String bucket, boolean findFreeName) throws InternalException, CloudException {
        while( bucket.endsWith("/") && !bucket.equals("/") ) {
            bucket = bucket.substring(0, bucket.length()-1);
        }
        if( bucket.equals("/") ) {
            throw new CloudException("Cannot create a directory named /");
        }
        String tmp = bucket;
        int idx = 1;

        while( exists(tmp) ) {
            if( !findFreeName ) {
                throw new CloudException("A directory already exists with the name " + bucket);
            }
            tmp = (bucket) + "-" + (idx++);
        }
        AtmosMethod method = new AtmosMethod(provider);

        idx = tmp.lastIndexOf("/");
        if( idx > -1 ) {
            bucket = tmp.substring(0, idx);
            tmp = tmp.substring(idx+1) + "/";
        }
        else {
            bucket = tmp;
            tmp = "/";
        }
        return method.create(tmp, bucket);
    }

    @Override
    public boolean exists(@Nonnull String bucket) throws InternalException, CloudException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void get(@Nullable String bucket, @Nonnull String object, @Nonnull File toFile, @Nullable FileTransfer transfer) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Blob getBucket(@Nonnull String bucketName) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Blob getObject(@Nullable String bucketName, @Nonnull String objectName) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Storage<Byte> getObjectSize(@Nullable String bucketName, @Nullable String objectName) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        return 100000;
    }

    @Override
    public Storage<Byte> getMaxObjectSize() throws InternalException, CloudException {
        return new Storage<Byte>(1000000000000L, Storage.BYTE);
    }

    @Override
    public int getMaxObjectsPerBucket() throws CloudException, InternalException {
        return 70000;
    }

    @Override
    public @Nonnull NameRules getBucketNameRules() throws CloudException, InternalException {
        return getObjectNameRules();
    }

    @Override
    public @Nonnull NameRules getObjectNameRules() throws CloudException, InternalException {
        return NameRules.getInstance(2, 100, true, true, true,
                new char[] { '#', '%', '^', '[', ']', '{', '}', '|', '\\', '"', ' ', ',', '<', '>', '-', '.', '_', '~', '!', '$', '\'', '(', ')', '*', '+', ',', ';', '=', ':'});
    }

    @Override
    public @Nonnull String getProviderTermForBucket(@Nonnull Locale locale) {
        return "namespace";
    }

    @Override
    public @Nonnull String getProviderTermForObject(@Nonnull Locale locale) {
        return "object";
    }

    @Override
    public boolean isPublic(@Nullable String bucket, @Nullable String object) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<Blob> list(@Nullable String bucket) throws CloudException, InternalException {
        AtmosMethod method = new AtmosMethod(provider);

        return method.list(bucket == null ? "/" : bucket);
    }

    @Override
    public void makePublic(@Nonnull String bucket) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Cannot make directories public");
    }

    @Override
    public void makePublic(@Nullable String bucket, @Nonnull String object) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Cannot make directories public");
    }

    @Override
    public void move(@Nullable String fromBucket, @Nullable String objectName, @Nullable String toBucket) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void put(@Nullable String bucket, @Nonnull String objectName, @Nonnull File file) throws InternalException, CloudException {
        if( bucket == null || bucket.equals("/") ) {
            throw new CloudException("You may not upload objects into the root");
        }
        Blob b = getObject(bucket, objectName);

        if( b == null ) {
            throw new CloudException("No such object: " + bucket + "/" + objectName);
        }
        AtmosMethod method = new AtmosMethod(provider);

        try {
            method.upload(bucket, objectName, new FileInputStream(file), new Storage<org.dasein.util.uom.storage.Byte>(file.length(), Storage.BYTE));
        }
        catch( IOException e ) {
            e.printStackTrace();
            throw new InternalException(e);
        }
    }

    @Override
    protected void put(@Nullable String bucketName, @Nonnull String objectName, @Nonnull String content) throws InternalException, CloudException {
        if( bucketName == null || bucketName.equals("/") ) {
            throw new CloudException("You may not upload objects into the root");
        }
        Blob b = getObject(bucketName, objectName);

        if( b == null ) {
            throw new CloudException("No such object: " + bucketName + "/" + objectName);
        }
        AtmosMethod method = new AtmosMethod(provider);

        method.upload(bucketName, objectName, "text/plain", content);
    }


    @Override
    public void removeBucket(@Nonnull String bucket) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeObject(@Nullable String bucket, @Nonnull String object) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public String renameBucket(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void renameObject(@Nullable String bucket, @Nonnull String oldName, @Nonnull String newName) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Blob upload(@Nonnull File sourceFile, @Nullable String bucket, @Nonnull String objectName) throws CloudException, InternalException {
        if( bucket == null || bucket.equals("/") ) {
            throw new CloudException("You may not upload objects into the root");
        }
        AtmosMethod method = new AtmosMethod(provider);

        try {
            return method.upload(bucket, objectName, new FileInputStream(sourceFile), new Storage<org.dasein.util.uom.storage.Byte>(sourceFile.length(), Storage.BYTE));
        }
        catch( IOException e ) {
            e.printStackTrace();
            throw new InternalException(e);
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
}
