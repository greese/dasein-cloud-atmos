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

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.storage.Blob;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

/**
 * Handles all RESTful interaction with the Atmos endpoint in order to perform specific REST operations.
 * <p>Created by George Reese: 10/5/12 9:14 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class AtmosMethod {
    public enum EndpointType {
        OBJECT, NAMESPACE;

        public String toEndpoint() {
            return (equals(EndpointType.NAMESPACE) ? "rest/namespace" : "rest/objects");
        }
    }

    static private final Logger logger = Atmos.getLogger(AtmosMethod.class);
    static private final Logger wire = Atmos.getWireLogger(AtmosMethod.class);

    private Atmos provider;

    public AtmosMethod(Atmos provider) { this.provider = provider; }

    protected void authorize(@Nonnull ProviderContext ctx, @Nonnull HttpRequestBase method, @Nonnull String contentType, @Nullable String range) throws CloudException, InternalException {
        ArrayList<Header> emcHeaders = new ArrayList<Header>();
        String date = provider.formatTime(System.currentTimeMillis());

        try {
            method.addHeader("x-emc-uid", new String(ctx.getAccessPublic(), "utf-8") + "/" + ctx.getAccountNumber());
        }
        catch( UnsupportedEncodingException e ) {
            logger.error("UTF-8 error: " + e.getMessage());
            throw new InternalException(e);
        }
        method.addHeader("x-emc-date", date);

        for( Header h : method.getAllHeaders() ) {
            if( h.getName().toLowerCase().startsWith("x-emc") ) {
                emcHeaders.add(h);
            }
        }
        String signatureString = toSignatureString(method, contentType, range == null ? "" : range, date, method.getURI(), emcHeaders);
        logger.debug(signatureString);
        String signature = sign(ctx, signatureString);
        logger.debug(signature);
        method.addHeader("x-emc-signature", signature);
    }

    public @Nonnull Blob create(@Nonnull String bucket, @Nonnull String name) throws CloudException, InternalException {
        list("/");
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + AtmosMethod.class.getName() + ".create(" + bucket + "," + name + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug("--------------------------------------------------------------------------------------");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            if( !bucket.endsWith("/") ) {
                bucket = bucket + "/";
            }
            if( !bucket.startsWith("/") ) {
                bucket = "/" + bucket;
            }
            if( !name.endsWith("/") ) {
                name = name + "/";
            }
            String endpoint = getEndpoint(ctx, EndpointType.NAMESPACE, bucket + name);
            HttpPost post = new HttpPost(endpoint);
            HttpClient client = getClient(endpoint);

            post.addHeader("Accept", "*/*");
            post.addHeader("Content-Type", "application/octet-stream");
            authorize(ctx, post, "application/octet-stream", null);
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int status = response.getStatusLine().getStatusCode();

            if( status == HttpServletResponse.SC_CREATED ) {
                return toBlob(ctx, response, bucket + name, null, null);
            }
            else {
                  throw new AtmosException(response);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + AtmosMethod.class.getName() + ".create()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }

    protected @Nonnull HttpClient getClient(String endpoint) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was specified for this request");
        }
        boolean ssl = endpoint.startsWith("https");
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "Dasein Cloud");

        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if( proxyHost != null ) {
                int port = 0;

                if( proxyPort != null && proxyPort.length() > 0 ) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        return new DefaultHttpClient(params);
    }

    protected @Nonnull String getEndpoint(@Nonnull ProviderContext ctx, @Nonnull EndpointType type, @Nullable String target) throws CloudException, InternalException {
        StringBuilder url = new StringBuilder();
        String endpoint = ctx.getEndpoint();

        if( endpoint == null ) {
            throw new CloudException("No endpoint was set for this request");
        }
        url.append(endpoint);
        if( target != null && !target.startsWith("/") ) {
            target = "/" + target;
        }
        else if( target == null ) {
            target = "/";
        }
        if( !endpoint.endsWith("/") ) {
            url.append("/");
        }
        url.append(type.toEndpoint());
        url.append(target);
        return url.toString();
    }

    public Iterable<Blob> list(@Nonnull String directory) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + AtmosMethod.class.getName() + ".list(" + directory + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug("--------------------------------------------------------------------------------------");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            if( !directory.startsWith("/") ) {
                directory = "/" + directory;
            }
            String endpoint = getEndpoint(ctx, EndpointType.NAMESPACE, directory);
            HttpGet get = new HttpGet(endpoint);
            HttpClient client = getClient(endpoint);

            get.addHeader("x-emc-include-meta", "true");
            get.addHeader("Accept", "text/xml");
            authorize(ctx, get, "", null);
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int status = response.getStatusLine().getStatusCode();

            if( status == HttpServletResponse.SC_OK ) {
                HttpEntity entity = response.getEntity();

                if( entity == null ) {
                    return Collections.emptyList();
                }
                try {
                    ArrayList<Blob> entries = new ArrayList<Blob>();
                    String xml = EntityUtils.toString(entity);

                    if( wire.isDebugEnabled() ) {
                        wire.debug(xml);
                    }
                    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder builder = factory.newDocumentBuilder();
                    Document doc = builder.parse(xml);

                    NodeList blocks = doc.getElementsByTagName("DirectoryEntry");

                    for( int i=0; i<blocks.getLength(); i++ ) {
                        Node entry = blocks.item(i);

                        Blob blob = toBlob(ctx, entry, directory);

                        if( blob != null ) {
                            entries.add(blob);
                        }
                    }
                    return entries;
                }
                catch( IOException e ) {
                    logger.error("I/O error reading from the cloud: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException("Error reading response from cloud");
                }
                catch( ParserConfigurationException e ) {
                    logger.error("Error with internal XML parser: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException("Error with internal XML parser");
                }
                catch( SAXException e ) {
                    logger.error("Invalid XML from the cloud: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException("Invalid XML from cloud");
                }
            }
            else {
                throw new AtmosException(response);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + AtmosMethod.class.getName() + ".list()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }

    }

    private @Nonnull String sign(@Nonnull ProviderContext ctx, @Nonnull String stringToSign) throws InternalException {
        try {
            Mac mac = Mac.getInstance("HmacSHA1");

            mac.init(new SecretKeySpec(ctx.getAccessPrivate(), "HmacSHA1"));
            return new String(Base64.encodeBase64(mac.doFinal(stringToSign.getBytes("UTF-8"))), "utf-8");
        }
        catch( NoSuchAlgorithmException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( InvalidKeyException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( IllegalStateException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( UnsupportedEncodingException e ) {
            logger.error(e);
            e.printStackTrace();
            throw new InternalException(e);
        }
    }

    private @Nonnull Blob toBlob(@Nonnull ProviderContext ctx, @Nonnull Node node, @Nonnull String directory) throws CloudException, InternalException {
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was specified for this request");
        }
        String objectId = null, objectName = null;
        NodeList attrs = node.getChildNodes();
        Storage<?> size = null;
        long created = 0L;

        for( int i=0; i<attrs.getLength(); i++ ) {
            Node attr = attrs.item(i);

            if( attr.getNodeName().equalsIgnoreCase("objectid") && attr.hasChildNodes() ) {
                objectId = attr.getFirstChild().getNodeValue().trim();
            }
            else if( attr.getNodeName().equalsIgnoreCase("filename") && attr.hasChildNodes() ) {
                objectName = attr.getFirstChild().getNodeValue().trim();
            }
            else if( (attr.getNodeName().equalsIgnoreCase("systemmetadatalist") || attr.getNodeName().equalsIgnoreCase("usermetadatalist")) && attr.hasChildNodes() ) {
                NodeList metas = attr.getChildNodes();

                for( int j=0; j<metas.getLength(); j++ ) {
                    Node meta = metas.item(j);
                    String name = null, value = null;

                    if( meta.hasChildNodes() ) {
                        NodeList childNodes = meta.getChildNodes();

                        for( int k=0; k<childNodes.getLength(); k++ ) {
                            Node child = childNodes.item(k);

                            if( child.getNodeName().equalsIgnoreCase("name") && child.hasChildNodes() ) {
                                name = child.getFirstChild().getNodeValue().trim();
                            }
                            else if( child.getNodeName().equalsIgnoreCase("value") && child.hasChildNodes() ) {
                                value = child.getFirstChild().getNodeValue().trim();
                            }
                        }
                    }
                    if( name != null && value != null ) {
                        if( name.equalsIgnoreCase("ctime") ) {
                            created = provider.parseTime(value);
                        }
                        else if( name.equalsIgnoreCase("size") ) {
                            size = new Storage<org.dasein.util.uom.storage.Byte>(Long.parseLong(value), Storage.BYTE);
                        }
                    }
                }
            }
        }
        if( objectId == null ) {
            return null;
        }
        if( objectName == null ) {
            return Blob.getInstance(regionId, "/rest/objects/" + objectId, directory, created);
        }
        else {
            return Blob.getInstance(regionId, "/rest/objects/" + objectId, directory, objectName, created, size);
        }
    }
    private @Nonnull Blob toBlob(@Nonnull ProviderContext ctx, @Nonnull HttpResponse response, @Nonnull String bucketName, @Nullable String objectName, @Nullable Storage<?> size) throws CloudException, InternalException {
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request");
        }
        Header location = response.getFirstHeader("location");
        String value = (location == null ? null : location.getValue());

        if( value == null ) {
            throw new CloudException(provider.getCloudName() + " indicated the object was created, but did not provide a location");
        }
        String objectId = toId(value);
        Header date = response.getFirstHeader("Date");
        long created = System.currentTimeMillis();

        if( date != null ) {
            value = date.getValue();
            if( value != null ) {
                created = provider.parseTime(value);
            }
        }
        if( objectName == null ) {
            return Blob.getInstance(regionId, "/rest/objects/" + objectId, bucketName, created);
        }
        else {
            if( size == null ) {
                size = new Storage<Gigabyte>(0, Storage.GIGABYTE);
            }
            return Blob.getInstance(regionId, "/rest/objects/" + objectId, bucketName, objectName, created, size);
        }
    }

    private @Nonnull String toId(@Nonnull String location) {
        return location.substring("/rest/objects/".length());
    }

    private @Nonnull String toSignatureString(@Nonnull HttpRequestBase method, @Nonnull String contentType, @Nonnull String range, @Nonnull String date, @Nonnull URI resource, @Nonnull List<Header> emcHeaders) {
        StringBuilder emcHeaderString = new StringBuilder();

        TreeSet<String> sorted = new TreeSet<String>();

        for( Header header : emcHeaders ) {
            sorted.add(header.getName().toLowerCase());
        }
        boolean first = true;
        for( String headerName : sorted ) {
            for( Header header : emcHeaders ) {
                if( header.getName().toLowerCase().equals(headerName) ) {
                    if( !first ) {
                        emcHeaderString.append("\n");
                    }
                    else {
                        first = false;
                    }
                    String val = header.getValue();

                    if( val == null ) {
                        val = "";
                    }
                    emcHeaderString.append(headerName);
                    emcHeaderString.append(":");
                    StringBuilder tmp = new StringBuilder();
                    for( char c : val.toCharArray() ) {
                        if( Character.isWhitespace(c) ) {
                            tmp.append(" ");
                        }
                        else {
                            tmp.append(c);
                        }
                    }
                    val = tmp.toString();
                    while( val.contains("  ") ) {
                        val = val.replaceAll("  ", " ");
                    }
                    emcHeaderString.append(val);
                }
            }
        }

        return (method.getMethod()  + "\n" + contentType + "\n" + range + "\n" + date + "\n" + resource.getRawPath().toLowerCase() + "\n" + emcHeaderString.toString());
    }

    public @Nonnull Blob upload(@Nonnull String bucket, @Nonnull String name, @Nonnull String contentType, @Nonnull String content) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + AtmosMethod.class.getName() + ".upload(" + bucket + "," + name + "," + contentType + ",[CONTENT])");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug("--------------------------------------------------------------------------------------");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            if( !bucket.endsWith("/") ) {
                bucket = bucket + "/";
            }
            if( !bucket.startsWith("/") ) {
                bucket = "/" + bucket;
            }
            String endpoint = getEndpoint(ctx, EndpointType.NAMESPACE, bucket + name);
            HttpPost post = new HttpPost(endpoint);
            HttpClient client = getClient(endpoint);

            authorize(ctx, post, contentType, null);
            try {
                post.setEntity(new StringEntity(content, contentType, "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                logger.error("Unsupported UTF-8 encoding: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException("Unsupported UTF-8 encoding");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int status = response.getStatusLine().getStatusCode();

            if( status == HttpServletResponse.SC_CREATED ) {
                return toBlob(ctx, response, bucket + name, null, null);
            }
            else {
                throw new AtmosException(response);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + AtmosMethod.class.getName() + ".upload()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }

    public @Nonnull Blob upload(@Nonnull String bucket, @Nonnull String name, @Nonnull InputStream input, Storage<?> size) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + AtmosMethod.class.getName() + ".upload(" + bucket + "," + name + ",[CONTENT]," + size + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug("--------------------------------------------------------------------------------------");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            if( !bucket.endsWith("/") ) {
                bucket = bucket + "/";
            }
            if( !bucket.startsWith("/") ) {
                bucket = "/" + bucket;
            }
            long length = size.convertTo(Storage.BYTE).getQuantity().longValue();
            String endpoint = getEndpoint(ctx, EndpointType.NAMESPACE, bucket + name);
            HttpPost post = new HttpPost(endpoint);
            HttpClient client = getClient(endpoint);

            authorize(ctx, post, "application/octet-stream", null);
            post.setEntity(new InputStreamEntity(input, length, ContentType.APPLICATION_OCTET_STREAM));
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int status = response.getStatusLine().getStatusCode();

            if( status == HttpServletResponse.SC_CREATED ) {
                return toBlob(ctx, response, bucket + name, null, null);
            }
            else {
                throw new AtmosException(response);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + AtmosMethod.class.getName() + ".upload()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }
}
