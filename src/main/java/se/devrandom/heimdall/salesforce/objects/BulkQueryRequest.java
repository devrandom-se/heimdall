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

public class BulkQueryRequest {
    public String id;
    public String operation;
    public String object;
    public String createdById;
    public String createdDate;
    public String systemModstamp;
    public String state;
    public String concurrencyMode;
    public String contentType;
    public String apiVersion;
    public String lineEnding;
    public String columnDelimiter;
    public String jobType;
    public Integer numberRecordsProcessed;
    public Integer retries;
    public Integer totalProcessingTime;


    public boolean isFinished() {
        return state.equals("Aborted") || state.equals("Failed") || state.equals("JobComplete");
    }

    public boolean wasSuccessful() {
        return state.equals("JobComplete");
    }

    public String getId() {
        return id;
    }
}
