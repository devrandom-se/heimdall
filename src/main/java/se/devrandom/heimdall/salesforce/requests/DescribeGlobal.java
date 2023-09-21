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
package se.devrandom.heimdall.salesforce.requests;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import se.devrandom.heimdall.salesforce.SalesforceAccessToken;
import se.devrandom.heimdall.salesforce.objects.DescribeGlobalResult;

import java.util.List;

public class DescribeGlobal {
    public String encoding;
    public Integer maxBatchSize;
    public List<DescribeGlobalResult> sobjects;

    public static DescribeGlobal request(SalesforceAccessToken salesforceAccessToken, WebClient webClient,
                                         String apiVersion) {
        Mono<DescribeGlobal> monoBody = webClient
                .get()
                .uri(uriBuilder -> uriBuilder
                        .path("/services/data/" + apiVersion + "/sobjects/")
                        .build())
                .headers(httpHeaders ->
                        httpHeaders.setBearerAuth(salesforceAccessToken.accessToken)
                )
                .accept(MediaType.APPLICATION_JSON)
                .exchangeToMono(response -> {
                    if(response.statusCode().equals(HttpStatus.OK)) {
                        return response.bodyToMono(DescribeGlobal.class);
                    } else {
                        return response.createError();
                    }
                });
        return monoBody.block();
    }
}
