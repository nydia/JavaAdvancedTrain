/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package com.nydia.agent.log4j.core.util;

import com.nydia.agent.log4j.Logger;
import com.nydia.agent.log4j.status.StatusLogger;

import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Enumeration;

import com.nydia.agent.log4j.Logger;
import com.nydia.agent.log4j.Logger;


/**
 * Networking-related convenience methods.
 */
public final class NetUtils {

    private static final Logger LOGGER = StatusLogger.getLogger();
    private static final String UNKNOWN_LOCALHOST = "UNKNOWN_LOCALHOST";

    private NetUtils() {
        // empty
    }

    /**
     * This method gets the network name of the machine we are running on. Returns "UNKNOWN_LOCALHOST" in the unlikely
     * case where the host name cannot be found.
     *
     * @return String the name of the local host
     */
    public static String getLocalHostname() {
        try {
            final InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostName();
        } catch (final UnknownHostException uhe) {
            try {
                final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                while (interfaces.hasMoreElements()) {
                    final NetworkInterface nic = interfaces.nextElement();
                    final Enumeration<InetAddress> addresses = nic.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        final InetAddress address = addresses.nextElement();
                        if (!address.isLoopbackAddress()) {
                            final String hostname = address.getHostName();
                            if (hostname != null) {
                                return hostname;
                            }
                        }
                    }
                }
            } catch (final SocketException se) {
                LOGGER.error("Could not determine local host name", uhe);
                return UNKNOWN_LOCALHOST;
            }
            LOGGER.error("Could not determine local host name", uhe);
            return UNKNOWN_LOCALHOST;
        }
    }

    /**
     * Converts a URI string or file path to a URI object.
     * 
     * @param path the URI string or path
     * @return the URI object
     */
    public static URI toURI(final String path) {
        try {
            // Resolves absolute URI
            return new URI(path);
        } catch (final URISyntaxException e) {
            // A file path or a Apache Commons VFS URL might contain blanks.
            // A file path may start with a driver letter
            try {
                final URL url = new URL(path);
                return new URI(url.getProtocol(), url.getHost(), url.getPath(), null);
            } catch (MalformedURLException | URISyntaxException nestedEx) {
                return new File(path).toURI();
            }
        }
    }

}
