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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BulkQueryResult {

    private String id;
    private String operation;
    private String object;
    private String createdById;
    private String state;
    private String contentType;
    private String apiVersion;
    private String concurrencyMode;
    private String resultUrl;       // används för att ladda ner CSV
    private Integer recordCount;    // antal poster i resultatet
    private String errorMessage;    // eventuella felmeddelanden

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }

    public String getObject() { return object; }
    public void setObject(String object) { this.object = object; }

    public String getCreatedById() { return createdById; }
    public void setCreatedById(String createdById) { this.createdById = createdById; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    public String getApiVersion() { return apiVersion; }
    public void setApiVersion(String apiVersion) { this.apiVersion = apiVersion; }

    public String getConcurrencyMode() { return concurrencyMode; }
    public void setConcurrencyMode(String concurrencyMode) { this.concurrencyMode = concurrencyMode; }

    public String getResultUrl() { return resultUrl; }
    public void setResultUrl(String resultUrl) { this.resultUrl = resultUrl; }

    public Integer getRecordCount() { return recordCount == null ? 0 : recordCount; }
    public void setRecordCount(Integer recordCount) { this.recordCount = recordCount; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}