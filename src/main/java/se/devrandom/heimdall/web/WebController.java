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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import se.devrandom.heimdall.storage.S3Service;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller
public class WebController {

    @Autowired
    private RestoreService restoreService;

    @Autowired
    private S3Service s3Service;

    private static final int PAGE_SIZE = 50;

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
        int archivedRecords = restoreService.countAllArchivedRecords();

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
