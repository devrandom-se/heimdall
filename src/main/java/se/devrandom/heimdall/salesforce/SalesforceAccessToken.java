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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import org.springframework.lang.NonNull;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class SalesforceAccessToken {
    @NonNull
    public String accessToken;
    @NonNull
    public String instanceUrl;
    @NonNull
    public String id;
    @NonNull
    public String tokenType;
    @NonNull
    public String scope;
    @NonNull
    public String issuedAt;
    @NonNull
    public String signature;
    public SalesforceAccessToken() {}
}