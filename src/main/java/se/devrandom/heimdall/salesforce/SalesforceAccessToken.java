/*
 * Heimdall - Salesforce Backup Solution
 * Copyright (C) 2025 Johan Karlsteen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package se.devrandom.heimdall.salesforce;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.NonNull;

/**
 * OAuth token response from Salesforce. The JSON names are given per field with
 * {@code @JsonProperty} (from jackson-annotations, which Jackson 2 and Jackson 3 share)
 * rather than with a databind naming strategy, so the mapping doesn't depend on which
 * Jackson version Spring's codecs use.
 */
public class SalesforceAccessToken {
    @NonNull
    @JsonProperty("access_token")
    public String accessToken;
    @NonNull
    @JsonProperty("instance_url")
    public String instanceUrl;
    @NonNull
    public String id;
    @NonNull
    @JsonProperty("token_type")
    public String tokenType;
    @NonNull
    public String scope;
    @NonNull
    @JsonProperty("issued_at")
    public String issuedAt;
    @NonNull
    public String signature;
    public SalesforceAccessToken() {}
}