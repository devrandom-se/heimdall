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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import se.devrandom.heimdall.salesforce.objects.DescribeSObjectResult;
import se.devrandom.heimdall.salesforce.objects.Field;
import se.devrandom.heimdall.storage.S3Service;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Controller
public class WebController {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WebController.class);

    @Autowired
    private RestoreService restoreService;

    @Autowired
    private S3Service s3Service;

    @Autowired(required = false)
    private SandboxRestoreClient sandboxRestoreClient;

    private static final int PAGE_SIZE = 50;

    private record ReferenceValidationResult(Map<String, Object> cleanedFields, List<String> skippedFields) {}

    /**
     * Validate all reference-type field values against the sandbox.
     * Returns cleaned fields (invalid references removed) and a list of what was skipped.
     */
    private ReferenceValidationResult validateReferenceFields(
            String objectName, String recordId, Map<String, Object> fields) {
        DescribeSObjectResult describe = sandboxRestoreClient.describeSObject(objectName);
        Map<String, Field> fieldMap = describe.getFieldMap();

        // Collect reference field IDs grouped by target object type
        Map<String, Set<String>> refIdsByType = new LinkedHashMap<>();
        Map<String, String> fieldToRefType = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            Field field = fieldMap.get(entry.getKey());
            if (field != null && "reference".equalsIgnoreCase(field.type)
                    && field.referenceTo != null && !field.referenceTo.isEmpty()) {
                String val = String.valueOf(entry.getValue());
                if (!val.isEmpty()) {
                    String refType;
                    if (field.referenceTo.size() == 1) {
                        refType = field.referenceTo.get(0);
                    } else {
                        // Polymorphic: resolve target type from ID prefix
                        refType = sandboxRestoreClient.resolveObjectTypeByIdPrefix(val, field.referenceTo);
                        if (refType == null) {
                            // Can't resolve type — skip validation for this field
                            continue;
                        }
                    }
                    refIdsByType.computeIfAbsent(refType, k -> new LinkedHashSet<>()).add(val);
                    fieldToRefType.put(entry.getKey(), refType);
                }
            }
        }

        // Batch validate all lookups
        Map<String, Set<String>> validIds = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> entry : refIdsByType.entrySet()) {
            validIds.put(entry.getKey(),
                    sandboxRestoreClient.batchValidateLookups(entry.getKey(), entry.getValue()));
        }

        // Remove invalid references
        Map<String, Object> cleaned = new LinkedHashMap<>(fields);
        List<String> skipped = new ArrayList<>();

        for (Map.Entry<String, String> entry : fieldToRefType.entrySet()) {
            String fieldName = entry.getKey();
            String refType = entry.getValue();
            String val = String.valueOf(cleaned.get(fieldName));
            Set<String> valid = validIds.getOrDefault(refType, Set.of());
            if (!valid.contains(val)) {
                skipped.add(fieldName + " (" + val + " not found in " + refType + ")");
                cleaned.remove(fieldName);
            }
        }

        if (!skipped.isEmpty()) {
            log.warn("Skipped invalid reference fields for {} {}: {}", objectName, recordId, skipped);
        }

        return new ReferenceValidationResult(cleaned, skipped);
    }

    @GetMapping("/health")
    @ResponseBody
    public String health() {
        return "OK";
    }

    @GetMapping("/")
    public String index(Model model) {
        Map<String, RestoreService.ObjectStats> stats = restoreService.getObjectStatistics();

        int totalObjects = stats.size();
        int totalRecords = stats.values().stream().mapToInt(RestoreService.ObjectStats::getUniqueRecords).sum();
        int deletedRecords = stats.values().stream().mapToInt(RestoreService.ObjectStats::getDeletedCount).sum();
        int archivedRecords = stats.values().stream().mapToInt(RestoreService.ObjectStats::getArchivedCount).sum();

        model.addAttribute("stats", Map.of(
            "totalObjects", totalObjects,
            "totalRecords", totalRecords,
            "deletedRecords", deletedRecords,
            "archivedRecords", archivedRecords
        ));
        model.addAttribute("pageTitle", "Dashboard");

        return "index";
    }

    @GetMapping("/record")
    public String recordHistory(
            @RequestParam("id") String recordId,
            @RequestParam(value = "v", defaultValue = "0") int versionIndex,
            Model model) {

        List<RestoreService.RecordVersion> versions = restoreService.getRecordVersions(recordId);

        model.addAttribute("recordId", recordId);
        model.addAttribute("versions", versions);
        model.addAttribute("selectedVersionIndex", versionIndex);

        // Record status (works even if versions is empty)
        RestoreService.RecordStatus recordStatus = restoreService.classifyRecordStatus(versions);
        model.addAttribute("recordStatus", recordStatus);

        if (!versions.isEmpty()) {
            RestoreService.RecordVersion selectedVersion = versions.get(
                Math.min(versionIndex, versions.size() - 1)
            );
            model.addAttribute("selectedVersion", selectedVersion);
            model.addAttribute("objectName", selectedVersion.getObjectName());
            model.addAttribute("recordName", selectedVersion.getName());

            // Calculate diff with previous version if available
            if (versionIndex < versions.size() - 1) {
                RestoreService.RecordVersion previousVersion = versions.get(versionIndex + 1);
                List<RestoreService.FieldChange> diff = restoreService.calculateDiff(selectedVersion, previousVersion);
                model.addAttribute("diff", diff);
            }

            // Related files
            List<RestoreService.RelatedFile> relatedFiles = restoreService.getRelatedFiles(recordId);
            model.addAttribute("relatedFiles", relatedFiles);

            // Related child records grouped by object name
            List<RestoreService.RelatedRecord> relatedRecords = restoreService.getRelatedChildRecords(recordId);
            Map<String, List<RestoreService.RelatedRecord>> groupedRelatedRecords = relatedRecords.stream()
                .collect(Collectors.groupingBy(
                    RestoreService.RelatedRecord::getObjectName,
                    LinkedHashMap::new,
                    Collectors.toList()
                ));
            model.addAttribute("groupedRelatedRecords", groupedRelatedRecords);

            // Check if this is a ContentVersion with a downloadable file
            boolean isContentVersion = "ContentVersion".equals(selectedVersion.getObjectName());
            model.addAttribute("isContentVersion", isContentVersion);
            if (isContentVersion) {
                Map<String, Object> meta = selectedVersion.getMetadata();
                boolean hasFile = meta != null
                        && meta.get("Checksum") != null
                        && meta.get("FileExtension") != null;
                model.addAttribute("hasDownloadableFile", hasFile);
            }
        }

        // Restore history
        List<RestoreService.RestoreLogEntry> restoreHistory = restoreService.getRestoreHistory(recordId);
        model.addAttribute("restoreHistory", restoreHistory);

        model.addAttribute("pageTitle", "Record History");
        return "record";
    }

    @GetMapping("/api/download-url")
    public ResponseEntity<?> downloadUrl(@RequestParam("id") String recordId) {
        List<RestoreService.RecordVersion> versions = restoreService.getRecordVersions(recordId);
        if (versions.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        RestoreService.RecordVersion latest = versions.get(0);
        if (!"ContentVersion".equals(latest.getObjectName())) {
            return ResponseEntity.badRequest().body("Not a ContentVersion record");
        }

        Map<String, Object> meta = latest.getMetadata();
        if (meta == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No metadata available");
        }

        Object checksum = meta.get("Checksum");
        Object fileExtension = meta.get("FileExtension");
        if (checksum == null || fileExtension == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Missing Checksum or FileExtension");
        }

        String url = s3Service.generatePresignedDownloadUrl(
                String.valueOf(checksum),
                String.valueOf(fileExtension),
                Duration.ofMinutes(15));

        if (url == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("File not found in S3");
        }

        return ResponseEntity.status(HttpStatus.FOUND)
                .location(URI.create(url))
                .build();
    }

    @PostMapping("/api/compare")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> compareRecord(@RequestBody Map<String, Object> request) {
        String recordId = (String) request.get("recordId");
        Object periodObj = request.get("period");

        if (recordId == null || periodObj == null) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Missing recordId or period"));
        }

        int period;
        try {
            period = Integer.parseInt(String.valueOf(periodObj));
        } catch (NumberFormatException e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Invalid period format"));
        }

        if (sandboxRestoreClient == null || !sandboxRestoreClient.isConfigured()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("success", false, "error", "Sandbox restore not configured. Set heimdall.restore.sandbox-name and credentials."));
        }

        List<RestoreService.RecordVersion> versions = restoreService.getRecordVersions(recordId);
        if (versions.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("success", false, "error", "Record not found in backup"));
        }

        RestoreService.RecordVersion targetVersion = null;
        for (RestoreService.RecordVersion v : versions) {
            if (v.getPeriod() == period) {
                targetVersion = v;
                break;
            }
        }
        if (targetVersion == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("success", false, "error", "Version with period " + period + " not found"));
        }

        String objectName = targetVersion.getObjectName();
        Map<String, Object> backupFields = targetVersion.getMetadata();

        // Fetch current record from sandbox
        Map<String, Object> currentFields = sandboxRestoreClient.fetchRecord(objectName, recordId);
        boolean recordExists = currentFields != null;
        boolean inRecycleBin = false;
        if (!recordExists) {
            inRecycleBin = sandboxRestoreClient.isInRecycleBin(objectName, recordId);
        }

        // Get updateable/createable field names
        Set<String> updateable = sandboxRestoreClient.getUpdateableFieldNames(objectName);
        Set<String> createable = sandboxRestoreClient.getCreateableFieldNames(objectName);

        // Build diff
        List<Map<String, Object>> diff = new ArrayList<>();
        Set<String> allKeys = new java.util.TreeSet<>();
        allKeys.addAll(backupFields.keySet());
        if (currentFields != null) allKeys.addAll(currentFields.keySet());

        for (String key : allKeys) {
            Object backupVal = backupFields.get(key);
            Object currentVal = currentFields != null ? currentFields.get(key) : null;

            String backupStr = backupVal != null ? String.valueOf(backupVal) : "";
            String currentStr = currentVal != null ? String.valueOf(currentVal) : "";

            if (!backupStr.equals(currentStr)) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("field", key);
                entry.put("backupValue", backupStr);
                entry.put("currentValue", currentStr);
                entry.put("updateable", recordExists ? updateable.contains(key) : createable.contains(key));
                diff.add(entry);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", true);
        result.put("recordExists", recordExists);
        result.put("inRecycleBin", inRecycleBin);
        result.put("objectName", objectName);
        result.put("diff", diff);
        result.put("sandboxName", sandboxRestoreClient.getSandboxName());
        result.put("lightningUrl", sandboxRestoreClient.getLightningUrl());
        return ResponseEntity.ok(result);
    }

    @PostMapping("/api/restore")
    @ResponseBody
    @SuppressWarnings("unchecked")
    public ResponseEntity<Map<String, Object>> restoreRecord(@RequestBody Map<String, Object> request) {
        String recordId = (String) request.get("recordId");
        Object periodObj = request.get("period");
        List<String> selectedFields = (List<String>) request.get("fields");
        Map<String, Object> fieldValues = (Map<String, Object>) request.get("fieldValues");

        if (recordId == null || periodObj == null) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Missing recordId or period"));
        }

        int period;
        try {
            period = Integer.parseInt(String.valueOf(periodObj));
        } catch (NumberFormatException e) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Invalid period format"));
        }

        if (sandboxRestoreClient == null || !sandboxRestoreClient.isConfigured()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("success", false, "error", "Sandbox restore not configured. Set heimdall.restore.sandbox-name and credentials."));
        }

        List<RestoreService.RecordVersion> versions = restoreService.getRecordVersions(recordId);
        if (versions.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("success", false, "error", "Record not found in backup"));
        }

        RestoreService.RecordVersion targetVersion = null;
        for (RestoreService.RecordVersion v : versions) {
            if (v.getPeriod() == period) {
                targetVersion = v;
                break;
            }
        }
        if (targetVersion == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("success", false, "error", "Version with period " + period + " not found"));
        }

        String objectName = targetVersion.getObjectName();
        Map<String, Object> metadata = targetVersion.getMetadata();

        SandboxRestoreClient.RestoreResult result;

        if (fieldValues != null && !fieldValues.isEmpty()) {
            // Restore with user-edited values — filter to createable/updateable
            Map<String, Object> sandboxRecord = sandboxRestoreClient.fetchRecord(objectName, recordId);
            boolean exists = sandboxRecord != null;

            Set<String> allowedFields = exists
                    ? sandboxRestoreClient.getUpdateableFieldNames(objectName)
                    : sandboxRestoreClient.getCreateableFieldNames(objectName);

            Map<String, Object> filtered = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : fieldValues.entrySet()) {
                if (allowedFields.contains(entry.getKey())) {
                    Object value = entry.getValue();
                    if (value == null) continue;
                    if (value instanceof String s && s.isEmpty()) continue;
                    filtered.put(entry.getKey(), value);
                }
            }

            // Validate reference fields — remove any pointing to non-existent records
            ReferenceValidationResult refValidation = validateReferenceFields(objectName, recordId, filtered);
            filtered = new LinkedHashMap<>(refValidation.cleanedFields());
            List<String> skippedRefs = refValidation.skippedFields();

            log.info("Restore with fieldValues for {} {}: sending {} fields{}: {}",
                    objectName, recordId, filtered.size(),
                    skippedRefs.isEmpty() ? "" : " (skipped " + skippedRefs.size() + " invalid refs)",
                    filtered.keySet());

            if (exists) {
                result = sandboxRestoreClient.updateFields(objectName, recordId, filtered);
            } else {
                result = sandboxRestoreClient.createRecord(objectName, filtered);
            }
            // For create: pass null so restore log shows "Created as ..." instead of "Updated fields: ..."
            List<String> fieldNamesList = exists ? new ArrayList<>(filtered.keySet()) : null;
            if (result.success()) {
                restoreService.logRestore(recordId, result.newId(), objectName, period, sandboxRestoreClient.getSandboxName(), fieldNamesList);
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", true);
                response.put("newId", result.newId());
                if (!skippedRefs.isEmpty()) {
                    response.put("skippedFields", skippedRefs);
                }
                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", false);
                response.put("error", result.error());
                if (!skippedRefs.isEmpty()) {
                    response.put("skippedFields", skippedRefs);
                }
                return ResponseEntity.ok(response);
            }
        } else if (selectedFields != null && !selectedFields.isEmpty()) {
            // Selective restore: PATCH only chosen fields
            Map<String, Object> fieldsToRestore = new LinkedHashMap<>();
            for (String field : selectedFields) {
                if (metadata.containsKey(field)) {
                    fieldsToRestore.put(field, metadata.get(field));
                }
            }
            log.info("Selective restore for {} {}: sending {} fields: {}",
                    objectName, recordId, fieldsToRestore.size(), fieldsToRestore.keySet());
            result = sandboxRestoreClient.updateFields(objectName, recordId, fieldsToRestore);
            // If record is deleted, fall back to creating new with all createable fields
            if (!result.success() && result.error() != null
                    && (result.error().contains("ENTITY_IS_DELETED") || result.error().contains("NOT_FOUND"))) {
                Map<String, Object> filteredFields = sandboxRestoreClient.filterToCreateableFields(objectName, metadata);
                result = sandboxRestoreClient.restoreRecord(objectName, recordId, filteredFields);
            }
        } else {
            // Full restore (deleted record): filter to createable and validate references
            Map<String, Object> filteredFields = sandboxRestoreClient.filterToCreateableFields(objectName, metadata);

            ReferenceValidationResult refValidation = validateReferenceFields(objectName, recordId, filteredFields);
            filteredFields = new LinkedHashMap<>(refValidation.cleanedFields());
            List<String> skippedRefs = refValidation.skippedFields();

            log.info("Full restore for {} {}: sending {} fields (from {} backup fields){}: {}",
                    objectName, recordId, filteredFields.size(), metadata.size(),
                    skippedRefs.isEmpty() ? "" : " (skipped " + skippedRefs.size() + " invalid refs)",
                    filteredFields.keySet());
            result = sandboxRestoreClient.restoreRecord(objectName, recordId, filteredFields);
        }

        if (result.success()) {
            restoreService.logRestore(recordId, result.newId(), objectName, period, sandboxRestoreClient.getSandboxName(), selectedFields);
            return ResponseEntity.ok(Map.of("success", true, "newId", result.newId()));
        } else {
            return ResponseEntity.ok(Map.of("success", false, "error", result.error()));
        }
    }

    @GetMapping("/restore")
    public String restorePage(@RequestParam(value = "id", required = false) String recordId,
                              @RequestParam(value = "period", required = false) Integer period,
                              Model model) {
        model.addAttribute("recordId", recordId);
        model.addAttribute("period", period);
        model.addAttribute("pageTitle", "Restore");
        return "restore";
    }

    @GetMapping("/api/restore-form")
    @ResponseBody
    public ResponseEntity<RestoreFormData> restoreForm(@RequestParam("id") String recordId,
                                                       @RequestParam("period") int period) {
        if (sandboxRestoreClient == null || !sandboxRestoreClient.isConfigured()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        List<RestoreService.RecordVersion> versions = restoreService.getRecordVersions(recordId);
        if (versions.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        RestoreService.RecordVersion targetVersion = null;
        for (RestoreService.RecordVersion v : versions) {
            if (v.getPeriod() == period) {
                targetVersion = v;
                break;
            }
        }
        if (targetVersion == null) {
            return ResponseEntity.notFound().build();
        }

        String objectName = targetVersion.getObjectName();
        Map<String, Object> backupFields = targetVersion.getMetadata();

        // Describe the object
        DescribeSObjectResult describe = sandboxRestoreClient.describeSObject(objectName);
        Map<String, Field> fieldMap = describe.getFieldMap();

        // Fetch sandbox record
        Map<String, Object> sandboxRecord = sandboxRestoreClient.fetchRecord(objectName, recordId);
        boolean recordExists = sandboxRecord != null;
        boolean inRecycleBin = false;
        if (!recordExists) {
            inRecycleBin = sandboxRestoreClient.isInRecycleBin(objectName, recordId);
        }

        // Collect lookup IDs per target object type for batch validation
        Map<String, Set<String>> lookupIdsByType = new LinkedHashMap<>();
        // Track resolved polymorphic types per field name
        Map<String, String> polymorphicResolvedType = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : backupFields.entrySet()) {
            Field field = fieldMap.get(entry.getKey());
            if (field != null && "reference".equalsIgnoreCase(field.type)
                    && entry.getValue() != null && !String.valueOf(entry.getValue()).isEmpty()) {
                List<String> refTo = field.referenceTo;
                if (refTo != null && refTo.size() == 1) {
                    lookupIdsByType.computeIfAbsent(refTo.get(0), k -> new LinkedHashSet<>())
                            .add(String.valueOf(entry.getValue()));
                } else if (refTo != null && refTo.size() > 1) {
                    // Polymorphic: resolve target type from ID prefix
                    String val = String.valueOf(entry.getValue());
                    String resolvedType = sandboxRestoreClient.resolveObjectTypeByIdPrefix(val, refTo);
                    if (resolvedType != null) {
                        polymorphicResolvedType.put(entry.getKey(), resolvedType);
                        lookupIdsByType.computeIfAbsent(resolvedType, k -> new LinkedHashSet<>()).add(val);
                    }
                }
            }
        }

        // Batch validate lookups
        Map<String, Set<String>> validLookupIds = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> entry : lookupIdsByType.entrySet()) {
            validLookupIds.put(entry.getKey(),
                    sandboxRestoreClient.batchValidateLookups(entry.getKey(), entry.getValue()));
        }

        // Build field entries
        List<RestoreFormData.FieldEntry> fieldEntries = new ArrayList<>();
        int warningCount = 0;

        for (Map.Entry<String, Object> entry : backupFields.entrySet()) {
            String fieldName = entry.getKey();
            Object backupValue = entry.getValue();
            Field field = fieldMap.get(fieldName);

            RestoreFormData.FieldEntry fe = new RestoreFormData.FieldEntry();
            fe.setName(fieldName);
            fe.setLabel(field != null && field.label != null ? field.label : fieldName);
            fe.setType(field != null ? field.type : "string");
            fe.setBackupValue(backupValue);
            fe.setSandboxValue(sandboxRecord != null ? sandboxRecord.get(fieldName) : null);

            if (field != null) {
                boolean editable = recordExists
                        ? Boolean.TRUE.equals(field.updateable)
                        : Boolean.TRUE.equals(field.createable);
                fe.setEditable(editable);
                fe.setRequired(!Boolean.TRUE.equals(field.nillable) && field.defaultValue == null);

                // Lookup validation
                if ("reference".equalsIgnoreCase(field.type)) {
                    fe.setReferenceTo(field.referenceTo);
                    String valStr = backupValue != null ? String.valueOf(backupValue) : "";
                    if (!valStr.isEmpty() && field.referenceTo != null && field.referenceTo.size() == 1) {
                        String targetType = field.referenceTo.get(0);
                        Set<String> valid = validLookupIds.getOrDefault(targetType, Set.of());
                        fe.setLookupValid(valid.contains(valStr));
                        if (!fe.isLookupValid()) {
                            fe.setValidationStatus("warning");
                            fe.setValidationMessage("Referenced " + targetType + " does not exist in sandbox");
                            warningCount++;
                        }
                    } else if (!valStr.isEmpty() && field.referenceTo != null && field.referenceTo.size() > 1) {
                        // Polymorphic lookup — validate using resolved type
                        String resolvedType = polymorphicResolvedType.get(fieldName);
                        if (resolvedType != null) {
                            fe.setLookupTargetName(resolvedType);
                            Set<String> valid = validLookupIds.getOrDefault(resolvedType, Set.of());
                            fe.setLookupValid(valid.contains(valStr));
                            if (!fe.isLookupValid()) {
                                fe.setValidationStatus("warning");
                                fe.setValidationMessage("Referenced " + resolvedType + " does not exist in sandbox");
                                warningCount++;
                            }
                        } else {
                            // Could not resolve type from ID prefix
                            fe.setLookupValid(true);
                            fe.setValidationStatus("warning");
                            fe.setValidationMessage("Polymorphic lookup — could not determine target object type from ID prefix");
                            warningCount++;
                        }
                    } else {
                        fe.setLookupValid(true);
                    }
                }

                // Picklist validation
                if (("picklist".equalsIgnoreCase(field.type) || "multipicklist".equalsIgnoreCase(field.type))
                        && field.picklistValues != null) {
                    List<RestoreFormData.PicklistOption> options = new ArrayList<>();
                    Set<String> activeValues = new HashSet<>();
                    for (Field.PicklistValue pv : field.picklistValues) {
                        RestoreFormData.PicklistOption opt = new RestoreFormData.PicklistOption();
                        opt.setValue(pv.value);
                        opt.setLabel(pv.label != null ? pv.label : pv.value);
                        opt.setActive(Boolean.TRUE.equals(pv.active));
                        options.add(opt);
                        if (Boolean.TRUE.equals(pv.active)) {
                            activeValues.add(pv.value);
                        }
                    }
                    fe.setPicklistValues(options);

                    String valStr = backupValue != null ? String.valueOf(backupValue) : "";
                    if (!valStr.isEmpty()) {
                        if ("multipicklist".equalsIgnoreCase(field.type)) {
                            String[] vals = valStr.split(";");
                            boolean allValid = true;
                            for (String v : vals) {
                                if (!activeValues.contains(v.trim())) {
                                    allValid = false;
                                    break;
                                }
                            }
                            fe.setPicklistValid(allValid);
                        } else {
                            fe.setPicklistValid(activeValues.contains(valStr));
                        }
                        if (!fe.isPicklistValid()) {
                            fe.setValidationStatus("warning");
                            fe.setValidationMessage("Value '" + valStr + "' is not an active picklist value");
                            warningCount++;
                        }
                    } else {
                        fe.setPicklistValid(true);
                    }
                }
            } else {
                fe.setEditable(false);
            }

            if (fe.getValidationStatus() == null) {
                fe.setValidationStatus("ok");
            }

            fieldEntries.add(fe);
        }

        RestoreFormData data = new RestoreFormData();
        data.setRecordId(recordId);
        data.setPeriod(period);
        data.setObjectName(objectName);
        data.setRecordName(targetVersion.getName());
        data.setRecordExistsInSandbox(recordExists);
        data.setInRecycleBin(inRecycleBin);
        data.setSandboxName(sandboxRestoreClient.getSandboxName());
        data.setLightningUrl(sandboxRestoreClient.getLightningUrl());
        data.setWarningCount(warningCount);
        data.setFields(fieldEntries);

        return ResponseEntity.ok(data);
    }

    @PostMapping("/api/lookup-search")
    @ResponseBody
    public ResponseEntity<List<Map<String, String>>> lookupSearch(@RequestBody Map<String, String> request) {
        if (sandboxRestoreClient == null || !sandboxRestoreClient.isConfigured()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        String objectName = request.get("objectName");
        String searchTerm = request.get("searchTerm");
        if (objectName == null || searchTerm == null || searchTerm.length() < 2) {
            return ResponseEntity.badRequest().build();
        }

        List<Map<String, String>> results = sandboxRestoreClient.searchLookup(objectName, searchTerm);
        return ResponseEntity.ok(results);
    }

    @GetMapping("/deleted")
    public String deletedRecords(
            @RequestParam(value = "object", required = false) String objectName,
            @RequestParam(value = "page", defaultValue = "0") int page,
            Model model) {

        List<String> objects = restoreService.getObjectsWithDeletedRecords();
        model.addAttribute("objects", objects);
        model.addAttribute("selectedObject", objectName);

        if (objectName != null && !objectName.isEmpty()) {
            List<RestoreService.DeletedRecord> records = restoreService.getDeletedRecords(objectName, page, PAGE_SIZE);
            int totalCount = restoreService.countDeletedRecords(objectName);
            int totalPages = (int) Math.ceil((double) totalCount / PAGE_SIZE);

            model.addAttribute("records", records);
            model.addAttribute("currentPage", page);
            model.addAttribute("totalPages", totalPages);
        } else {
            model.addAttribute("records", List.of());
        }

        model.addAttribute("pageTitle", "Deleted Records");
        return "deleted";
    }

    @GetMapping("/archived")
    public String archivedRecords(
            @RequestParam(value = "object", required = false) String objectName,
            @RequestParam(value = "page", defaultValue = "0") int page,
            Model model) {

        List<String> objects = restoreService.getObjectsWithArchivedRecords();
        model.addAttribute("objects", objects);
        model.addAttribute("selectedObject", objectName);

        if (objectName != null && !objectName.isEmpty()) {
            List<RestoreService.DeletedRecord> records = restoreService.getArchivedRecords(objectName, page, PAGE_SIZE);
            int totalCount = restoreService.countArchivedRecords(objectName);
            int totalPages = (int) Math.ceil((double) totalCount / PAGE_SIZE);

            model.addAttribute("records", records);
            model.addAttribute("currentPage", page);
            model.addAttribute("totalPages", totalPages);
        } else {
            model.addAttribute("records", List.of());
        }

        model.addAttribute("pageTitle", "Archived Records");
        return "archived";
    }

    @GetMapping("/objects")
    public String objectBrowser(Model model) {
        Map<String, RestoreService.ObjectStats> stats = restoreService.getObjectStatistics();
        model.addAttribute("objectStats", stats.values());
        model.addAttribute("pageTitle", "Object Browser");
        return "objects";
    }
}
