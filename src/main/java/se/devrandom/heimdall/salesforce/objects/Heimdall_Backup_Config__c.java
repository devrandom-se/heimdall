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

import java.util.Date;

public class Heimdall_Backup_Config__c extends SObject {
    public static final String BACKUP = "Backup";
    public static final String NO_BACKUP = "No Backup";
    public static final String NOT_REPLICABLE = "Not Replicable";

    @SalesforceField(externalId = true)
    public String ObjectName__c;

    @SalesforceField public Date Last_Backup_Run__c;
    @SalesforceField public String Status__c;

    // Smart COUNT optimization fields
    @SalesforceField public Boolean CheckCountFirst__c;
    @SalesforceField public Integer ConsecutiveEmptyRuns__c;
    @SalesforceField public Date LastNonEmptyRun__c;
    @SalesforceField public String ChangeFrequency__c;
    @SalesforceField public Double AverageRecordsPerRun__c;

    // Archive fields
    @SalesforceField public Boolean Archive__c;
    @SalesforceField public Integer Archive_Age_Days__c;
    @SalesforceField public String Archive_Expression__c;
    @SalesforceField public String Archive_Action__c;

    public String getObjectName__c() {
        return ObjectName__c;
    }

    public void setObjectName__c(String objectName__c) {
        ObjectName__c = objectName__c;
        setName(objectName__c);  // Keep Name in sync with ObjectName__c
    }

    public Date getLast_Backup_Run__c() {
        return Last_Backup_Run__c;
    }

    public void setLast_Backup_Run__c(Date last_Backup_Run__c) {
        Last_Backup_Run__c = last_Backup_Run__c;
    }

    public String getStatus__c() {
        return Status__c;
    }

    public void setStatus__c(String status__c) {
        Status__c = status__c;
    }

    public Boolean getCheckCountFirst__c() {
        return CheckCountFirst__c;
    }

    public void setCheckCountFirst__c(Boolean checkCountFirst__c) {
        CheckCountFirst__c = checkCountFirst__c;
    }

    public Integer getConsecutiveEmptyRuns__c() {
        return ConsecutiveEmptyRuns__c;
    }

    public void setConsecutiveEmptyRuns__c(Integer consecutiveEmptyRuns__c) {
        ConsecutiveEmptyRuns__c = consecutiveEmptyRuns__c;
    }

    public Date getLastNonEmptyRun__c() {
        return LastNonEmptyRun__c;
    }

    public void setLastNonEmptyRun__c(Date lastNonEmptyRun__c) {
        LastNonEmptyRun__c = lastNonEmptyRun__c;
    }

    public String getChangeFrequency__c() {
        return ChangeFrequency__c;
    }

    public void setChangeFrequency__c(String changeFrequency__c) {
        ChangeFrequency__c = changeFrequency__c;
    }

    public Double getAverageRecordsPerRun__c() {
        return AverageRecordsPerRun__c;
    }

    public void setAverageRecordsPerRun__c(Double averageRecordsPerRun__c) {
        AverageRecordsPerRun__c = averageRecordsPerRun__c;
    }

    public Boolean getArchive__c() {
        return Archive__c;
    }

    public void setArchive__c(Boolean archive__c) {
        Archive__c = archive__c;
    }

    public Integer getArchive_Age_Days__c() {
        return Archive_Age_Days__c;
    }

    public void setArchive_Age_Days__c(Integer archive_Age_Days__c) {
        Archive_Age_Days__c = archive_Age_Days__c;
    }

    public String getArchive_Expression__c() {
        return Archive_Expression__c;
    }

    public void setArchive_Expression__c(String archive_Expression__c) {
        Archive_Expression__c = archive_Expression__c;
    }

    public String getArchive_Action__c() {
        return Archive_Action__c;
    }

    public void setArchive_Action__c(String archive_Action__c) {
        Archive_Action__c = archive_Action__c;
    }

    public Heimdall_Backup_Config__c() {}

    public Heimdall_Backup_Config__c(String objectName, String status) {
        super(objectName);
        ObjectName__c = objectName;
        this.Status__c = status;
    }

    public boolean equalsOnImportantFields(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final Heimdall_Backup_Config__c other = (Heimdall_Backup_Config__c) obj;
        if (getObjectName__c() == null ?
                other.getObjectName__c() != null :
                !getObjectName__c().equals(other.getObjectName__c()) ||
            (getStatus__c() == null ?
                other.getStatus__c() != null :
                !getStatus__c().equals(other.getStatus__c()))
        ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Heimdall_Backup_Config__c{" +
                "Id='" + Id + '\'' +
                ", Name='" + Name + '\'' +
                ", ObjectName__c='" + ObjectName__c + '\'' +
                ", Status__c='" + Status__c + '\'' +
                ", Last_Backup_Run__c=" + Last_Backup_Run__c +
                '}';
    }
}
