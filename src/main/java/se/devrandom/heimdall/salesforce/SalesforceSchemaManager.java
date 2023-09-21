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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import se.devrandom.heimdall.config.SalesforceCredentials;
import se.devrandom.heimdall.salesforce.objects.DescribeSObjectResult;
import se.devrandom.heimdall.salesforce.objects.Field;

import java.util.*;
import java.util.stream.Collectors;

public class SalesforceSchemaManager {
    private static final Logger log = LoggerFactory.getLogger(SalesforceSchemaManager.class);

    private static final String OBJECT_FULL_NAME = "Heimdall_Backup_Config__c";

    private final WebClient webClient;
    private final SalesforceAccessToken accessToken;
    private final SalesforceCredentials credentials;

    private record FieldDefinition(String name, Map<String, Object> metadata) {}

    private static final List<FieldDefinition> EXPECTED_FIELDS = List.of(
            new FieldDefinition("ObjectName__c", Map.of(
                    "type", "Text", "label", "Object Name", "length", 80,
                    "externalId", true, "unique", true, "required", true)),
            new FieldDefinition("Status__c", Map.ofEntries(
                    Map.entry("type", "Picklist"), Map.entry("label", "Status"),
                    Map.entry("valueSet", picklistValueSet("Backup", "No Backup", "Not Replicable")))),
            new FieldDefinition("Last_Backup_Run__c", Map.of(
                    "type", "Date", "label", "Last Backup Run")),
            new FieldDefinition("CheckCountFirst__c", Map.of(
                    "type", "Checkbox", "label", "Check COUNT First", "defaultValue", false)),
            new FieldDefinition("ConsecutiveEmptyRuns__c", Map.of(
                    "type", "Number", "label", "Consecutive Empty Runs",
                    "precision", 3, "scale", 0)),
            new FieldDefinition("LastNonEmptyRun__c", Map.of(
                    "type", "Date", "label", "Last Non-Empty Run")),
            new FieldDefinition("ChangeFrequency__c", Map.ofEntries(
                    Map.entry("type", "Picklist"), Map.entry("label", "Change Frequency"),
                    Map.entry("valueSet", picklistValueSet("High", "Medium", "Low", "Static")))),
            new FieldDefinition("AverageRecordsPerRun__c", Map.of(
                    "type", "Number", "label", "Average Records Per Run",
                    "precision", 18, "scale", 2)),
            new FieldDefinition("Archive__c", Map.of(
                    "type", "Checkbox", "label", "Archive", "defaultValue", false)),
            new FieldDefinition("Archive_Age_Days__c", Map.of(
                    "type", "Number", "label", "Archive Age Days",
                    "precision", 5, "scale", 0)),
            new FieldDefinition("Archive_Expression__c", Map.of(
                    "type", "Text", "label", "Archive Expression", "length", 255)),
            new FieldDefinition("Archive_Action__c", Map.ofEntries(
                    Map.entry("type", "Picklist"), Map.entry("label", "Archive Action"),
                    Map.entry("valueSet", picklistValueSet("Archive Only", "Archive and Delete"))))
    );

    public SalesforceSchemaManager(WebClient webClient, SalesforceAccessToken accessToken,
                                   SalesforceCredentials credentials) {
        this.webClient = webClient;
        this.accessToken = accessToken;
        this.credentials = credentials;
    }

    public void ensureSchema() {
        try {
            log.info("Checking Salesforce schema for {}...", OBJECT_FULL_NAME);
            DescribeSObjectResult describe = describeObject();

            if (describe == null) {
                log.info("{} does not exist, creating object and all fields...", OBJECT_FULL_NAME);
                createObject();
                int created = createMissingFields(EXPECTED_FIELDS);
                log.info("Schema auto-migration complete: created object + {} fields", created);
            } else {
                Set<String> existingFieldNames = describe.fields.stream()
                        .map(f -> f.name.toLowerCase())
                        .collect(Collectors.toSet());

                List<FieldDefinition> missingFields = EXPECTED_FIELDS.stream()
                        .filter(fd -> !existingFieldNames.contains(fd.name().toLowerCase()))
                        .toList();

                if (missingFields.isEmpty()) {
                    log.info("Schema OK, all fields present on {}", OBJECT_FULL_NAME);
                } else {
                    log.info("{} exists but missing {} fields, creating...", OBJECT_FULL_NAME, missingFields.size());
                    int created = createMissingFields(missingFields);
                    log.info("Schema auto-migration complete: created {} fields", created);
                }
            }
        } catch (Exception e) {
            log.error("Auto-migration failed: {}. Deploy metadata manually from sf-metadata/ directory " +
                    "(run: sf project deploy start --source-dir sf-metadata/force-app). " +
                    "Required permission: 'Customize Application' or 'Modify All Data'.", e.getMessage());
        }
    }

    private DescribeSObjectResult describeObject() {
        try {
            return webClient
                    .get()
                    .uri("/services/data/" + credentials.getApiVersion() + "/sobjects/" + OBJECT_FULL_NAME + "/describe")
                    .headers(headers -> headers.setBearerAuth(accessToken.accessToken))
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(DescribeSObjectResult.class)
                    .block();
        } catch (WebClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return null;
            }
            throw e;
        }
    }

    private void createObject() {
        Map<String, Object> nameField = Map.of("type", "Text", "label", "Object Backup Name");
        Map<String, Object> metadata = Map.ofEntries(
                Map.entry("label", "Heimdall Backup Config"),
                Map.entry("pluralLabel", "Heimdall Backup Configs"),
                Map.entry("deploymentStatus", "Deployed"),
                Map.entry("sharingModel", "ReadWrite"),
                Map.entry("nameField", nameField),
                Map.entry("enableBulkApi", true),
                Map.entry("enableReports", true),
                Map.entry("enableSearch", true)
        );

        Map<String, Object> body = Map.of(
                "FullName", OBJECT_FULL_NAME,
                "Metadata", metadata
        );

        String response = webClient
                .post()
                .uri("/services/data/" + credentials.getApiVersion() + "/tooling/sobjects/CustomObject")
                .headers(headers -> headers.setBearerAuth(accessToken.accessToken))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        log.info("Created custom object {}: {}", OBJECT_FULL_NAME, response);
    }

    private int createMissingFields(List<FieldDefinition> fields) {
        int created = 0;
        for (FieldDefinition field : fields) {
            try {
                createField(field);
                created++;
            } catch (Exception e) {
                log.error("Failed to create field {}: {}", field.name(), e.getMessage());
            }
        }
        return created;
    }

    private void createField(FieldDefinition field) {
        Map<String, Object> body = Map.of(
                "FullName", OBJECT_FULL_NAME + "." + field.name(),
                "Metadata", field.metadata()
        );

        String response = webClient
                .post()
                .uri("/services/data/" + credentials.getApiVersion() + "/tooling/sobjects/CustomField")
                .headers(headers -> headers.setBearerAuth(accessToken.accessToken))
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        log.info("Created field {}.{}: {}", OBJECT_FULL_NAME, field.name(), response);
    }

    private static Map<String, Object> picklistValueSet(String... values) {
        List<Map<String, Object>> valueList = new ArrayList<>();
        for (String value : values) {
            valueList.add(Map.of(
                    "fullName", value,
                    "default", false,
                    "label", value
            ));
        }
        Map<String, Object> valueSetDefinition = Map.of(
                "sorted", false,
                "value", valueList
        );
        return Map.of(
                "restricted", true,
                "valueSetDefinition", valueSetDefinition
        );
    }
}
