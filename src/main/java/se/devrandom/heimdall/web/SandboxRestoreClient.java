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
package se.devrandom.heimdall.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import se.devrandom.heimdall.salesforce.SalesforceAccessToken;
import se.devrandom.heimdall.salesforce.objects.DescribeSObjectResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;

@Service
@ConditionalOnExpression("!'${heimdall.restore.sandbox-name:}'.isEmpty()")
public class SandboxRestoreClient {

    private static final Logger log = LoggerFactory.getLogger(SandboxRestoreClient.class);

    @Value("${heimdall.restore.sandbox-name}")
    private String sandboxName;

    @Value("${heimdall.restore.sandbox-login-url:https://test.salesforce.com}")
    private String loginUrl;

    @Value("${heimdall.restore.sandbox-username}")
    private String sandboxUsername;

    @Value("${heimdall.restore.sandbox-client-id}")
    private String clientId;

    @Value("${heimdall.restore.sandbox-client-secret:}")
    private String clientSecret;

    @Value("${heimdall.restore.sandbox-grant-type:${salesforce.grant-type}}")
    private String grantType;

    @Value("${heimdall.restore.sandbox-jwt-key-file:${salesforce.jwt-key-file:}}")
    private String jwtKeyFile;

    @Value("${salesforce.api-version}")
    private String apiVersion;

    private WebClient webClient;
    private String instanceUrl;
    private String accessToken;
    private boolean configured;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final ConcurrentHashMap<String, DescribeSObjectResult> describeCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> keyPrefixCache = new ConcurrentHashMap<>();

    @PostConstruct
    private void init() {
        this.webClient = WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(20 * 1024 * 1024))
                .build();

        try {
            authenticate();
            configured = true;
            log.info("Sandbox restore client initialized for sandbox '{}' (instance: {})", sandboxName, instanceUrl);
        } catch (Exception e) {
            log.error("Failed to authenticate to sandbox '{}': {}", sandboxName, e.getMessage());
            configured = false;
        }
    }

    private void authenticate() {
        if ("urn:ietf:params:oauth:grant-type:jwt-bearer".equals(grantType)) {
            loginWithJWT();
        } else {
            loginWithClientCredentials();
        }
    }

    private void loginWithJWT() {
        try {
            PrivateKey privateKey = readPrivateKey(jwtKeyFile);

            long expMillis = System.currentTimeMillis() + (5 * 60 * 1000);

            String jwt = Jwts.builder()
                    .issuer(clientId)
                    .subject(sandboxUsername)
                    .audience().add(loginUrl).and()
                    .expiration(new Date(expMillis))
                    .signWith(privateKey)
                    .compact();

            LinkedMultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
            formData.add("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
            formData.add("assertion", jwt);

            SalesforceAccessToken token = webClient.post()
                    .uri(loginUrl + "/services/oauth2/token")
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                    .body(BodyInserters.fromFormData(formData))
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(SalesforceAccessToken.class)
                    .block();

            this.instanceUrl = token.instanceUrl;
            this.accessToken = token.accessToken;
            log.info("Sandbox JWT auth successful for user: {}", sandboxUsername);
        } catch (Exception e) {
            throw new RuntimeException("Sandbox JWT authentication failed", e);
        }
    }

    private void loginWithClientCredentials() {
        LinkedMultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
        formData.add("grant_type", "client_credentials");
        formData.add("client_id", clientId);
        formData.add("client_secret", clientSecret);

        SalesforceAccessToken token = webClient.post()
                .uri(loginUrl + "/services/oauth2/token")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .body(BodyInserters.fromFormData(formData))
                .accept(MediaType.APPLICATION_JSON)
                .acceptCharset(StandardCharsets.UTF_8)
                .retrieve()
                .bodyToMono(SalesforceAccessToken.class)
                .block();

        this.instanceUrl = token.instanceUrl;
        this.accessToken = token.accessToken;
        log.info("Sandbox client_credentials auth successful");
    }

    private PrivateKey readPrivateKey(String keyFilePath) throws Exception {
        if (keyFilePath == null || keyFilePath.isEmpty()) {
            throw new IllegalArgumentException("JWT key file path is not configured");
        }

        Path keyPath = Paths.get(keyFilePath);
        if (!Files.exists(keyPath)) {
            throw new java.io.IOException("JWT key file not found: " + keyFilePath);
        }

        String keyContent = Files.readString(keyPath, StandardCharsets.UTF_8);
        keyContent = keyContent
                .replaceAll("-----BEGIN.*-----", "")
                .replaceAll("-----END.*-----", "")
                .replaceAll("\\s+", "");

        byte[] keyBytes = Base64.getDecoder().decode(keyContent);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(keySpec);
    }

    public DescribeSObjectResult describeSObject(String objectName) {
        return describeCache.computeIfAbsent(objectName, name -> {
            String url = instanceUrl + "/services/data/" + apiVersion + "/sobjects/" + name + "/describe/";
            String json = webClient.get()
                    .uri(url)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            try {
                return objectMapper.readValue(json, DescribeSObjectResult.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse describe result for " + name, e);
            }
        });
    }

    public String getKeyPrefix(String objectName) {
        return keyPrefixCache.computeIfAbsent(objectName, name -> {
            DescribeSObjectResult desc = describeSObject(name);
            return desc.keyPrefix != null ? desc.keyPrefix : "";
        });
    }

    public String resolveObjectTypeByIdPrefix(String id, List<String> candidateTypes) {
        if (id == null || id.length() < 3 || candidateTypes == null || candidateTypes.isEmpty()) {
            return null;
        }
        String prefix = id.substring(0, 3);
        for (String type : candidateTypes) {
            try {
                String kp = getKeyPrefix(type);
                if (prefix.equals(kp)) {
                    return type;
                }
            } catch (Exception e) {
                log.debug("Could not describe {} for key prefix resolution: {}", type, e.getMessage());
            }
        }
        return null;
    }

    public Set<String> getCreateableFieldNames(String objectName) {
        return describeSObject(objectName).getCreateableFieldNames();
    }

    public Set<String> getUpdateableFieldNames(String objectName) {
        return describeSObject(objectName).getUpdateableFieldNames();
    }

    public Map<String, Object> filterToCreateableFields(String objectName, Map<String, Object> allFields) {
        Set<String> createable = getCreateableFieldNames(objectName);
        Map<String, Object> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : allFields.entrySet()) {
            if (!createable.contains(entry.getKey())) continue;
            Object value = entry.getValue();
            if (value == null) continue;
            if (value instanceof String s && s.isEmpty()) continue;
            filtered.put(entry.getKey(), value);
        }
        return filtered;
    }

    /**
     * Fetch a record from the sandbox by ID.
     * Returns field map, or null if not found.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> fetchRecord(String objectName, String recordId) {
        String url = instanceUrl + "/services/data/" + apiVersion + "/sobjects/" + objectName + "/" + recordId;
        try {
            String json = webClient.get()
                    .uri(url)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            Map<String, Object> record = objectMapper.readValue(json, Map.class);
            record.remove("attributes");
            return record;
        } catch (WebClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return null;
            }
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse record response", e);
        }
    }

    /**
     * Check if a deleted record is in the recycle bin (soft-deleted) via SOQL queryAll.
     */
    @SuppressWarnings("unchecked")
    public boolean isInRecycleBin(String objectName, String recordId) {
        String soql = "SELECT Id FROM " + objectName + " WHERE Id = '" + recordId + "' AND IsDeleted = true";
        String url = instanceUrl + "/services/data/" + apiVersion + "/queryAll/?q=" + java.net.URLEncoder.encode(soql, StandardCharsets.UTF_8);
        try {
            String json = webClient.get()
                    .uri(java.net.URI.create(url))
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            Map<String, Object> result = objectMapper.readValue(json, Map.class);
            Integer totalSize = (Integer) result.get("totalSize");
            return totalSize != null && totalSize > 0;
        } catch (Exception e) {
            log.warn("Failed to check recycle bin for {}: {}", recordId, e.getMessage());
            return false;
        }
    }

    /**
     * Update specific fields on an existing record (PATCH).
     */
    public RestoreResult updateFields(String objectName, String recordId, Map<String, Object> fields) {
        return updateRecord(objectName, recordId, fields);
    }

    /**
     * Restore a record: update existing (PATCH) if it exists, create new (POST) if not.
     * Used for deleted records where there's no interactive diff.
     */
    @SuppressWarnings("unchecked")
    public RestoreResult restoreRecord(String objectName, String originalId, Map<String, Object> fields) {
        if (originalId != null && !originalId.isEmpty()) {
            RestoreResult updateResult = updateRecord(objectName, originalId, fields);
            if (updateResult.success()) {
                return updateResult;
            }
            String err = updateResult.error();
            if (err != null && (err.contains("NOT_FOUND") || err.contains("ENTITY_IS_DELETED") || err.contains("INVALID_CROSS_REFERENCE_KEY"))) {
                log.info("Record {} not found or deleted in sandbox, creating new", originalId);
                return createRecord(objectName, fields);
            }
            return updateResult;
        }
        return createRecord(objectName, fields);
    }

    private RestoreResult updateRecord(String objectName, String recordId, Map<String, Object> fields) {
        String url = instanceUrl + "/services/data/" + apiVersion + "/sobjects/" + objectName + "/" + recordId;
        try {
            String jsonPayload = new com.fasterxml.jackson.databind.ObjectMapper()
                    .writerWithDefaultPrettyPrinter().writeValueAsString(fields);
            log.debug("PATCH {} {} payload ({} fields):\n{}", objectName, recordId, fields.size(), jsonPayload);

            webClient.patch()
                    .uri(url)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .header("Sforce-Duplicate-Rule-Header", "allowSave=true")
                    .bodyValue(fields)
                    .retrieve()
                    .toBodilessEntity()
                    .block();

            // PATCH returns 204 No Content on success
            return RestoreResult.success(recordId);
        } catch (WebClientResponseException e) {
            return handleSalesforceError(objectName, e);
        } catch (Exception e) {
            return RestoreResult.failure(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public RestoreResult createRecord(String objectName, Map<String, Object> fields) {
        String url = instanceUrl + "/services/data/" + apiVersion + "/sobjects/" + objectName + "/";
        try {
            String jsonPayload = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(fields);
            log.debug("POST {} payload ({} fields):\n{}", objectName, fields.size(), jsonPayload);

            // Use exchangeToMono to handle all response codes ourselves
            // (Salesforce may return 400 for DUPLICATES_DETECTED even with allowSave header)
            org.springframework.web.reactive.function.client.ClientResponse clientResponse = webClient.post()
                    .uri(url)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .header("Sforce-Duplicate-Rule-Header", "allowSave=true")
                    .accept(MediaType.APPLICATION_JSON)
                    .bodyValue(fields)
                    .exchange()
                    .block();

            String responseJson = clientResponse.bodyToMono(String.class).block();
            int statusCode = clientResponse.statusCode().value();
            log.debug("POST {} response (HTTP {}): {}", objectName, statusCode, responseJson);

            if (statusCode >= 200 && statusCode < 300) {
                Map<String, Object> response = objectMapper.readValue(responseJson, Map.class);
                Boolean success = (Boolean) response.get("success");
                if (Boolean.TRUE.equals(success)) {
                    return RestoreResult.success((String) response.get("id"));
                } else {
                    List<Map<String, Object>> errors = (List<Map<String, Object>>) response.get("errors");
                    String errorMsg = errors != null && !errors.isEmpty()
                            ? String.valueOf(errors.get(0).get("message"))
                            : "Unknown error";
                    return RestoreResult.failure(errorMsg);
                }
            }

            // Error response — parse Salesforce error array
            log.error("Salesforce POST error for {} (HTTP {}): {}", objectName, statusCode, responseJson);
            try {
                List<?> errors = objectMapper.readValue(responseJson, List.class);
                if (!errors.isEmpty() && errors.get(0) instanceof Map<?,?> firstError) {
                    String sfCode = String.valueOf(firstError.get("errorCode"));
                    String sfMsg = String.valueOf(firstError.get("message"));

                    List<?> sfFields = (List<?>) firstError.get("fields");
                    String fieldsInfo = (sfFields != null && !sfFields.isEmpty())
                            ? " [fields: " + sfFields + "]" : "";
                    return RestoreResult.failure(sfCode + ": " + sfMsg + fieldsInfo);
                }
            } catch (Exception parseEx) {
                // Fall through
            }
            return RestoreResult.failure("HTTP " + statusCode + ": " + responseJson);
        } catch (Exception e) {
            return RestoreResult.failure(e.getMessage());
        }
    }

    private RestoreResult handleSalesforceError(String objectName, WebClientResponseException e) {
        String body = e.getResponseBodyAsString();
        log.error("Salesforce REST error for {}: {}", objectName, body);
        try {
            List<?> errors = objectMapper.readValue(body, List.class);
            if (!errors.isEmpty() && errors.get(0) instanceof Map<?,?> firstError) {
                String sfMsg = String.valueOf(firstError.get("message"));
                String sfCode = String.valueOf(firstError.get("errorCode"));
                List<?> sfFields = (List<?>) firstError.get("fields");
                String fieldsInfo = (sfFields != null && !sfFields.isEmpty())
                        ? " [fields: " + sfFields + "]" : "";
                return RestoreResult.failure(sfCode + ": " + sfMsg + fieldsInfo);
            }
        } catch (Exception parseEx) {
            // Fall through
        }
        return RestoreResult.failure(e.getStatusCode() + ": " + body);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> executeSoqlQuery(String soql) {
        String url = instanceUrl + "/services/data/" + apiVersion + "/query/?q="
                + java.net.URLEncoder.encode(soql, java.nio.charset.StandardCharsets.UTF_8);
        try {
            String json = webClient.get()
                    .uri(java.net.URI.create(url))
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            Map<String, Object> result = objectMapper.readValue(json, Map.class);
            List<Map<String, Object>> records = (List<Map<String, Object>>) result.get("records");
            if (records != null) {
                records.forEach(r -> r.remove("attributes"));
            }
            return records != null ? records : List.of();
        } catch (Exception e) {
            log.error("SOQL query failed: {}", e.getMessage());
            throw new RuntimeException("SOQL query failed", e);
        }
    }

    public Set<String> batchValidateLookups(String objectType, Set<String> ids) {
        Set<String> validIds = new HashSet<>();
        if (ids == null || ids.isEmpty()) return validIds;

        List<String> idList = new ArrayList<>(ids);
        int batchSize = 100;
        for (int i = 0; i < idList.size(); i += batchSize) {
            List<String> batch = idList.subList(i, Math.min(i + batchSize, idList.size()));
            String inClause = batch.stream()
                    .map(id -> "'" + id + "'")
                    .collect(java.util.stream.Collectors.joining(","));
            String soql = "SELECT Id FROM " + objectType + " WHERE Id IN (" + inClause + ")";
            try {
                List<Map<String, Object>> records = executeSoqlQuery(soql);
                for (Map<String, Object> rec : records) {
                    Object id = rec.get("Id");
                    if (id != null) validIds.add(String.valueOf(id));
                }
            } catch (Exception e) {
                log.warn("Batch validate lookups failed for {}: {}", objectType, e.getMessage());
            }
        }
        return validIds;
    }

    public List<Map<String, String>> searchLookup(String objectType, String searchTerm) {
        String safeTerm = searchTerm.replace("'", "\\'").replace("%", "\\%");
        String soql = "SELECT Id, Name FROM " + objectType + " WHERE Name LIKE '%" + safeTerm + "%' LIMIT 10";
        try {
            List<Map<String, Object>> records = executeSoqlQuery(soql);
            List<Map<String, String>> results = new ArrayList<>();
            for (Map<String, Object> rec : records) {
                Map<String, String> entry = new LinkedHashMap<>();
                entry.put("id", String.valueOf(rec.get("Id")));
                entry.put("name", rec.get("Name") != null ? String.valueOf(rec.get("Name")) : "");
                results.add(entry);
            }
            return results;
        } catch (Exception e) {
            log.warn("Lookup search failed for {}: {}", objectType, e.getMessage());
            return List.of();
        }
    }

    public boolean isConfigured() {
        return configured;
    }

    public String getSandboxName() {
        return sandboxName;
    }

    public String getLightningUrl() {
        if (instanceUrl == null) return null;
        return instanceUrl.replace(".my.salesforce.com", ".lightning.force.com");
    }

    public record RestoreResult(boolean success, String newId, String error) {
        static RestoreResult success(String newId) {
            return new RestoreResult(true, newId, null);
        }
        static RestoreResult failure(String error) {
            return new RestoreResult(false, null, error);
        }
    }
}
