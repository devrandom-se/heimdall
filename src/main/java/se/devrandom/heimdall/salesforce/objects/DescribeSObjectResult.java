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
package se.devrandom.heimdall.salesforce.objects;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DescribeSObjectResult {
    public static final String ADDRESS = "address";
    public static final String BASE64 = "base64";
    public static final String TIER = "tier";
    public static final String SYSTEM_MODSTAMP = "SystemModstamp";
    public static final String CREATED_DATE = "CreatedDate";
    public String name;
    public Boolean replicateable;
    public List<ChildRelationship> childRelationships;
    public List<Field> fields;
    public String keyPrefix;

    public Map<String, Field> getFieldMap() {
        Map<String, Field> map = new LinkedHashMap<>();
        if (fields != null) for (Field f : fields) map.put(f.name, f);
        return map;
    }

    public Boolean hasSystemModstampField() {
        for(Field f : fields) {
            if(f.name.equalsIgnoreCase("SystemModstamp")) {
                return true;
            }
        }
        return false;
    }

    public Boolean hasIsDeletedField() {
        for(Field f : fields) {
            if(f.name.equalsIgnoreCase("IsDeleted")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get names of all reference (lookup) fields on this object.
     * Used to store relationship IDs in metadata regardless of field naming convention.
     */
    public Set<String> getCreateableFieldNames() {
        Set<String> names = new HashSet<>();
        if (fields == null) return names;
        for (Field f : fields) {
            if (Boolean.TRUE.equals(f.createable)) {
                names.add(f.name);
            }
        }
        return names;
    }

    public Set<String> getUpdateableFieldNames() {
        Set<String> names = new HashSet<>();
        if (fields == null) return names;
        for (Field f : fields) {
            if (Boolean.TRUE.equals(f.updateable)) {
                names.add(f.name);
            }
        }
        return names;
    }

    public Set<String> getReferenceFieldNames() {
        Set<String> refFields = new HashSet<>();
        if (fields == null) return refFields;
        for (Field f : fields) {
            if ("reference".equalsIgnoreCase(f.type)) {
                refFields.add(f.name);
            }
        }
        return refFields;
    }
    /**
     * Build the field list for SOQL queries, with SystemModstamp and Id last.
     */
    private List<String> getFieldList() {
        List<String> fieldNames = new ArrayList<>();
        for(Field f : fields) {
            // Exclude special types and fields we'll add at the end
            if (!(f.type.equalsIgnoreCase(ADDRESS) ||
                    f.type.equalsIgnoreCase(BASE64) ||
                    f.name.equalsIgnoreCase(TIER) ||
                    f.name.equalsIgnoreCase("Id") ||
                    f.name.equalsIgnoreCase("SystemModstamp"))) {
                fieldNames.add(f.name);
            }
        }

        // Add SystemModstamp and Id LAST - this allows validation that CSV wasn't truncated
        // by checking that the last column (Id) of the last row is not empty
        if (hasSystemModstampField()) {
            fieldNames.add("SystemModstamp");
        }
        fieldNames.add("Id");

        return fieldNames;
    }

    /**
     * Generate SOQL query with checkpoint for incremental backup
     * @param queryAll true for deleted records (queryAll), false for active records
     * @param lastModstamp last SystemModstamp from previous run (formatted for SOQL)
     * @param lastId last Id from previous run
     * @param retrieveLimit max records to retrieve
     */
    public String generateSOQLQuery(Boolean queryAll, String lastModstamp, String lastId, Integer retrieveLimit) {
        List<String> fieldNames = getFieldList();
        String availableDateField = hasSystemModstampField() ? SYSTEM_MODSTAMP : CREATED_DATE;

        return String.format("SELECT %s FROM %s WHERE %s((%s = %s AND Id > '%s') OR %s > %s) ORDER BY %s ASC, Id ASC LIMIT %d",
                String.join(",", fieldNames),
                name,
                queryAll ? "IsDeleted = true AND " : "",
                availableDateField,
                lastModstamp,
                lastId,
                availableDateField,
                lastModstamp,
                availableDateField,
                retrieveLimit);
    }

    /**
     * Generate SOQL query for archive with CreatedDate threshold and optional expression filter.
     * Uses SystemModstamp checkpoint for pagination (same as backup) but adds CreatedDate filter
     * to only include records older than the threshold.
     *
     * @param createdDateThreshold SOQL-formatted datetime threshold for CreatedDate
     * @param archiveExpression optional additional WHERE clause expression (e.g. "Status = 'Closed'")
     * @param lastModstamp last SystemModstamp from previous archive run (formatted for SOQL)
     * @param lastId last Id from previous archive run
     * @param retrieveLimit max records to retrieve
     */
    public String generateArchiveSOQLQuery(String createdDateThreshold, String archiveExpression,
                                            String lastModstamp, String lastId, Integer retrieveLimit) {
        List<String> fieldNames = getFieldList();
        String availableDateField = hasSystemModstampField() ? SYSTEM_MODSTAMP : CREATED_DATE;

        StringBuilder query = new StringBuilder();
        query.append(String.format("SELECT %s FROM %s WHERE CreatedDate < %s",
                String.join(",", fieldNames), name, createdDateThreshold));

        if (archiveExpression != null && !archiveExpression.isBlank()) {
            query.append(String.format(" AND (%s)", archiveExpression));
        }

        query.append(String.format(" AND ((%s = %s AND Id > '%s') OR %s > %s)",
                availableDateField, lastModstamp, lastId,
                availableDateField, lastModstamp));

        query.append(String.format(" ORDER BY %s ASC, Id ASC LIMIT %d",
                availableDateField, retrieveLimit));

        return query.toString();
    }
}
