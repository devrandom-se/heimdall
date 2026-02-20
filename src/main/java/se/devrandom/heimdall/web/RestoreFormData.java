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

import java.util.List;

public class RestoreFormData {
    private String recordId;
    private int period;
    private String objectName;
    private String recordName;
    private boolean recordExistsInSandbox;
    private boolean inRecycleBin;
    private String sandboxName;
    private String lightningUrl;
    private int warningCount;
    private List<FieldEntry> fields;

    // Getters and setters
    public String getRecordId() { return recordId; }
    public void setRecordId(String recordId) { this.recordId = recordId; }
    public int getPeriod() { return period; }
    public void setPeriod(int period) { this.period = period; }
    public String getObjectName() { return objectName; }
    public void setObjectName(String objectName) { this.objectName = objectName; }
    public String getRecordName() { return recordName; }
    public void setRecordName(String recordName) { this.recordName = recordName; }
    public boolean isRecordExistsInSandbox() { return recordExistsInSandbox; }
    public void setRecordExistsInSandbox(boolean recordExistsInSandbox) { this.recordExistsInSandbox = recordExistsInSandbox; }
    public boolean isInRecycleBin() { return inRecycleBin; }
    public void setInRecycleBin(boolean inRecycleBin) { this.inRecycleBin = inRecycleBin; }
    public String getSandboxName() { return sandboxName; }
    public void setSandboxName(String sandboxName) { this.sandboxName = sandboxName; }
    public String getLightningUrl() { return lightningUrl; }
    public void setLightningUrl(String lightningUrl) { this.lightningUrl = lightningUrl; }
    public int getWarningCount() { return warningCount; }
    public void setWarningCount(int warningCount) { this.warningCount = warningCount; }
    public List<FieldEntry> getFields() { return fields; }
    public void setFields(List<FieldEntry> fields) { this.fields = fields; }

    public static class FieldEntry {
        private String name;
        private String label;
        private String type;
        private boolean editable;
        private boolean required;
        private Object backupValue;
        private Object sandboxValue;
        private List<String> referenceTo;
        private boolean lookupValid;
        private String lookupTargetName;
        private List<PicklistOption> picklistValues;
        private boolean picklistValid;
        private String validationStatus;
        private String validationMessage;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getLabel() { return label; }
        public void setLabel(String label) { this.label = label; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public boolean isEditable() { return editable; }
        public void setEditable(boolean editable) { this.editable = editable; }
        public boolean isRequired() { return required; }
        public void setRequired(boolean required) { this.required = required; }
        public Object getBackupValue() { return backupValue; }
        public void setBackupValue(Object backupValue) { this.backupValue = backupValue; }
        public Object getSandboxValue() { return sandboxValue; }
        public void setSandboxValue(Object sandboxValue) { this.sandboxValue = sandboxValue; }
        public List<String> getReferenceTo() { return referenceTo; }
        public void setReferenceTo(List<String> referenceTo) { this.referenceTo = referenceTo; }
        public boolean isLookupValid() { return lookupValid; }
        public void setLookupValid(boolean lookupValid) { this.lookupValid = lookupValid; }
        public String getLookupTargetName() { return lookupTargetName; }
        public void setLookupTargetName(String lookupTargetName) { this.lookupTargetName = lookupTargetName; }
        public List<PicklistOption> getPicklistValues() { return picklistValues; }
        public void setPicklistValues(List<PicklistOption> picklistValues) { this.picklistValues = picklistValues; }
        public boolean isPicklistValid() { return picklistValid; }
        public void setPicklistValid(boolean picklistValid) { this.picklistValid = picklistValid; }
        public String getValidationStatus() { return validationStatus; }
        public void setValidationStatus(String validationStatus) { this.validationStatus = validationStatus; }
        public String getValidationMessage() { return validationMessage; }
        public void setValidationMessage(String validationMessage) { this.validationMessage = validationMessage; }
    }

    public static class PicklistOption {
        private String value;
        private String label;
        private boolean active;

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public String getLabel() { return label; }
        public void setLabel(String label) { this.label = label; }
        public boolean isActive() { return active; }
        public void setActive(boolean active) { this.active = active; }
    }
}
