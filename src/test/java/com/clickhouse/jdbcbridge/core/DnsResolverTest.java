/**
 * Copyright 2019-2022, Zhichun Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clickhouse.jdbcbridge.core;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class DnsResolverTest {
    @Test(groups = { "unit" })
    public void testResolve() {
        String dns = "_sip._udp.sip.voice.google.com";
        assertNotNull(new DnsResolver().resolve(dns, true));
        assertNotNull(new DnsResolver().resolve(dns, false));

        dns = "_sip._udp.sip.voice.google.com.";
        assertNotNull(new DnsResolver().resolve(dns, true));
        assertNotNull(new DnsResolver().resolve(dns, false));
    }

    @Test(groups = { "unit" })
    public void testApply() {
        String host = "_sip._udp.sip.voice.google.com";
        String port = "5060";
        String hostAndPort = host + ":" + port;

        String dns = "_sip._udp.sip.voice.google.com";
        assertEquals(new DnsResolver().apply(dns), hostAndPort);
        assertEquals(new DnsResolver().apply("host:" + dns), host);
        assertEquals(new DnsResolver().apply("port:" + dns), port);

        dns = "_sip._udp.sip.voice.google.com.";
        assertEquals(new DnsResolver().apply(dns), hostAndPort);
        assertEquals(new DnsResolver().apply("host:" + dns), host);
        assertEquals(new DnsResolver().apply("port:" + dns), port);
    }
}