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

import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

/**
 * This class will replace host and port variables in given connection string by
 * resolving SRV records.
 * 
 * @since 2.0
 */
public class DnsResolver {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DnsResolver.class);

    private static final String PREFIX_HOST = "host:"; // just host
    private static final String PREFIX_PORT = "port:"; // just port

    // TODO a backend thread to reload datasource when its DNS changed
    // private final Map<String, String> dns2ds = new ConcurrentHashMap<>();

    private final Cache<String, Record[]> dnsCache = Caffeine.newBuilder().maximumSize(100)
            .expireAfterAccess(5, TimeUnit.MINUTES).build();

    public SRVRecord resolve(String srvDns, boolean basedOnWeight) {
        Record[] records = null;

        try {
            records = dnsCache.get(srvDns, k -> {
                Record[] results = null;

                try {
                    results = new Lookup(srvDns, Type.SRV).run();
                } catch (TextParseException e) {
                }

                return results;
            });
        } catch (Exception e) {
            log.warn("Not able to resolve given DNS query: [{}]", srvDns);
        }

        SRVRecord record = null;
        if (records != null) {
            if (basedOnWeight) {
                for (int i = 0; i < records.length; i++) {
                    SRVRecord rec = (SRVRecord) records[i];
                    if (record == null || record.getWeight() > rec.getWeight()) {
                        record = rec;
                    }
                }
            } else {
                record = (SRVRecord) records[0];
            }
        }

        return record;
    }

    public String apply(String dns) {
        boolean onlyHost = false;
        boolean onlyPort = false;

        String query = dns;
        if (dns.startsWith(PREFIX_HOST)) {
            onlyHost = true;
            query = dns.substring(PREFIX_HOST.length());
        } else if (dns.startsWith(PREFIX_PORT)) {
            onlyPort = true;
            query = dns.substring(PREFIX_PORT.length());
        }

        SRVRecord record = resolve(query, false);
        if (record != null) {
            if (onlyHost) {
                dns = record.getName().canonicalize().toString(true);
            } else if (onlyPort) {
                dns = String.valueOf(record.getPort());
            } else {
                dns = new StringBuilder().append(record.getName().canonicalize().toString(true)).append(':')
                        .append(record.getPort()).toString();
            }
        }

        return dns;
    }
}
