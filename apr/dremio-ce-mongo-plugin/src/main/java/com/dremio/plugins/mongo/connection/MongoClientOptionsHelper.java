package com.dremio.plugins.mongo.connection;

import com.mongodb.*;
import com.dremio.ssl.*;
import javax.net.*;
import javax.net.ssl.*;
import java.util.*;
import com.google.common.collect.*;
import java.nio.charset.*;
import java.net.*;
import java.io.*;

public final class MongoClientOptionsHelper
{
    public static MongoClientOptions newMongoClientOptions(final MongoClientURI mongoURI) {
        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder(mongoURI.getOptions());
        if (mongoURI.getOptions().isSslEnabled()) {
            final String connection = mongoURI.getURI();
            URI uri = null;
            try {
                uri = new URI(connection);
            }
            catch (URISyntaxException e) {
                throw new RuntimeException("Cannot decode properly URI " + connection, e);
            }
            final ListMultimap<String, String> parameters = getParameters(uri);
            final boolean sslInvalidHostnameAllowed = Boolean.parseBoolean(getLastValue(parameters, "sslInvalidHostnameAllowed"));
            builder.sslInvalidHostNameAllowed(sslInvalidHostnameAllowed);
            final boolean sslInvalidCertificateAllowed = Boolean.parseBoolean(getLastValue(parameters, "sslInvalidCertificateAllowed"));
            if (sslInvalidCertificateAllowed) {
                final SSLContext sslContext = SSLHelper.newAllTrustingSSLContext("TLS");
                builder.socketFactory((SocketFactory)sslContext.getSocketFactory());
            }
        }
        return builder.build();
    }
    
    private static String getLastValue(final ListMultimap<String, String> parameters, final String key) {
        final List<String> list = (List<String>)parameters.get(key);
        if (list == null || list.isEmpty()) {
            return null;
        }
        return list.get(list.size() - 1);
    }
    
    private static ListMultimap<String, String> getParameters(final URI uri) {
        final String query = uri.getQuery();
        final ListMultimap<String, String> parameters = ArrayListMultimap.create();
        final String[] split;
        final String[] items = split = query.split("&");
        for (final String item : split) {
            final String[] keyValue = item.split("=");
            if (keyValue.length != 0) {
                final String key = urlDecode(keyValue[0]);
                final String value = (keyValue.length > 1) ? urlDecode(keyValue[1]) : null;
                parameters.put(key, value);
            }
        }
        return parameters;
    }
    
    private static String urlDecode(final String s) {
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8.name());
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding not supported", e);
        }
    }
}
