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

import org.apache.http.HttpStatus;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.NameRules;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.atmos.Atmos;
import org.dasein.cloud.atmos.AtmosMethod;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.storage.AbstractBlobStoreSupport;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.uom.storage.Byte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
        while( bucket.startsWith("/") && !bucket.equals("/") ) {
            bucket = bucket.substring(1);
        }
        if( bucket.equals("/") ) {
            return true;
        }
        int idx = bucket.lastIndexOf("/");

        if( idx < 0 ) {
            for( Blob b : list(null) ) {
                String name = b.getBucketName();

                if( name != null && name.equals(bucket) ) {
                    return true;
                }
            }
            return false;
        }
        String root = bucket.substring(0, idx);

        for( Blob b : list(root) ) {
            String name = b.getBucketName();

            if( name != null && name.equals(bucket) ) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void get(@Nullable String bucket, @Nonnull String object, @Nonnull File toFile, @Nullable FileTransfer transfer) throws InternalException, CloudException {
        if( bucket == null ) {
            throw new CloudException("No bucket was specified");
        }
        IOException lastError = null;
        int attempts = 0;

        while( attempts < 5 ) {
            AtmosMethod method = new AtmosMethod(provider);

            try {
                InputStream input = method.download(bucket, object);

                try {
                    copy(input, new FileOutputStream(toFile), transfer);
                    return;
                }
                catch( FileNotFoundException e ) {
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                catch( IOException e ) {
                    lastError = e;
                    try { Thread.sleep(10000L); }
                    catch( InterruptedException ignore ) { }
                }
                finally {
                    input.close();
                }
            }
            catch( IOException e ) {
                e.printStackTrace();
                throw new CloudException(e);
            }
            attempts++;
        }
        if( lastError != null ) {
            lastError.printStackTrace();
            throw new InternalException(lastError);
        }
        else {
            throw new InternalException("Unknown error");
        }
    }

    @Override
    public Blob getBucket(@Nonnull String bucket) throws InternalException, CloudException {
        while( bucket.startsWith("/") && !bucket.equals("/") ) {
            bucket = bucket.substring(1);
        }
        if( bucket.equals("/") ) {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region was set for this request");
            }
            return Blob.getInstance(regionId, "/rest/namespace/", "/", 0);
        }
        int idx = bucket.lastIndexOf("/");

        if( idx < 0 ) {
            for( Blob b : list(null) ) {
                String name = b.getBucketName();

                if( name != null && name.equals(bucket) ) {
                    return b;
                }
            }
            return null;
        }
        String root = bucket.substring(0, idx);

        for( Blob b : list(root) ) {
            String name = b.getBucketName();

            if( name != null && name.equals(bucket) ) {
                return b;
            }
        }
        return null;
    }

    @Override
    public Blob getObject(@Nullable String bucketName, @Nonnull String objectName) throws InternalException, CloudException {
        if( bucketName == null ) {
            return null;
        }
        AtmosMethod method = new AtmosMethod(provider);

        return method.info(bucketName, objectName);
    }

    @Override
    public Storage<Byte> getObjectSize(@Nullable String bucketName, @Nullable String objectName) throws InternalException, CloudException {
        if( bucketName == null ) {
            throw new CloudException("No such object: /" + objectName);
        }
        AtmosMethod method = new AtmosMethod(provider);
        Blob object = method.info(bucketName, objectName);

        return (object == null ? null : object.getSize());
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
        try {
            AtmosMethod method = new AtmosMethod(provider);

            method.list("/");
            return true;
        }
        catch( CloudException e ) {
            if( e.getHttpCode() == HttpStatus.SC_FORBIDDEN ) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public @Nonnull Iterable<Blob> list(final @Nullable String bucket) throws CloudException, InternalException {
        PopulatorThread<Blob> populator = new PopulatorThread<Blob>(new JiteratorPopulator<Blob>() {
            @Override
            public void populate(@Nonnull Jiterator<Blob> iterator) throws Exception {
                try {
                    AtmosMethod method = new AtmosMethod(provider);

                    for( Blob b : method.list(bucket == null ? "/" : bucket) ) {
                        iterator.push(b);
                    }
                }
                finally {
                    provider.release();
                }
            }
        });

        provider.hold();
        populator.populate();
        return populator.getResult();
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
        AtmosMethod method = new AtmosMethod(provider);

        method.delete(bucket, null);
    }

    @Override
    public void removeObject(@Nullable String bucket, @Nonnull String object) throws CloudException, InternalException {
        if( bucket == null ) {
            throw new CloudException("No such object: /" + object);
        }
        AtmosMethod method = new AtmosMethod(provider);

        method.delete(bucket, object);
    }

    @Override
    public @Nonnull String renameBucket(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws CloudException, InternalException {
        Blob bucket = getBucket(oldName);

        if( bucket == null ) {
            throw new CloudException("No such object: /" + oldName);
        }
        while( oldName.startsWith("/") && !oldName.equals("/") ) {
            oldName = oldName.substring(1);
        }
        if( oldName.equals("/") ) {
            throw new CloudException("Cannot rename the root directory");
        }
        while( oldName.endsWith("/") ) {
            oldName = oldName.substring(0, oldName.length()-1);
        }
        while( newName.startsWith("/") && !newName.equals("/") ) {
            newName = newName.substring(1);
        }
        if( newName.equals("/") ) {
            throw new CloudException("Cannot rename a directory to the root");
        }
        while( newName.endsWith("/") ) {
            newName = newName.substring(0, newName.length()-1);
        }
        int idx = oldName.lastIndexOf("/");
        String oldRoot;

        if( idx < 0 ) {
            oldRoot = null;
        }
        else {
            oldRoot = oldName.substring(0, idx);
            oldName = oldName.substring(idx+1);
        }

        String newRoot;

        idx = newName.lastIndexOf("/");
        if( idx < 0 ) {
            newRoot = null;
        }
        else {
            newRoot = newName.substring(0, idx);
            newName = newName.substring(idx+1);
        }
        if( (oldRoot == null && newRoot == null) || (oldRoot != null && oldRoot.equals(newRoot)) ) {
            AtmosMethod method = new AtmosMethod(provider);

            method.rename("/", oldName, newName);
            return (oldRoot == null ? newName : oldRoot + "/" + newName);
        }
        else {
            // TODO: rename to new root
            return null;
        }
    }

    @Override
    public void renameObject(@Nullable String bucket, @Nonnull String oldName, @Nonnull String newName) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Blob upload(@Nonnull File sourceFile, @Nullable String bucket, @Nonnull String objectName) throws CloudException, InternalException {
        if( bucket == null || bucket.equals("/") ) {
            throw new OperationNotSupportedException("You may not upload objects into the root");
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
