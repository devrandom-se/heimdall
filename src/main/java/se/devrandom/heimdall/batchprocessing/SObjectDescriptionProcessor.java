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
package se.devrandom.heimdall.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import se.devrandom.heimdall.salesforce.SalesforceService;
import se.devrandom.heimdall.salesforce.objects.Heimdall_Backup_Config__c;
import se.devrandom.heimdall.salesforce.objects.DescribeGlobalResult;

import java.util.Set;

public class SObjectDescriptionProcessor
        implements ItemProcessor<DescribeGlobalResult, Heimdall_Backup_Config__c> {

    @Autowired
    private SalesforceService salesforceService;

    private static final Logger log = LoggerFactory.getLogger(SObjectDescriptionProcessor.class);

    // Default objects to backup when starting fresh - common Salesforce objects
    private static final Set<String> DEFAULT_BACKUP_OBJECTS = Set.of(
        // Core CRM objects
        "Account",
        "Contact",
        "Lead",
        "Opportunity",
        "Case",
        "Contract",
        // Activities
        "Task",
        "Event",
        // Products & Pricing
        "Product2",
        "Pricebook2",
        "PricebookEntry",
        "OpportunityLineItem",
        // Content
        "ContentVersion",
        "ContentDocument",
        // Users & Groups
        "User",
        // Campaigns
        "Campaign",
        "CampaignMember",
        // Other common objects
        "Order",
        "OrderItem",
        "Quote",
        "QuoteLineItem",
        "Asset",
        "Entitlement",
        "ServiceContract"
    );
    @Override
    public Heimdall_Backup_Config__c process(DescribeGlobalResult item) {
        log.trace(String.format("Processing %s (replicateable: %s)", item.name, item.replicateable));

        // Check if this object already exists - if so, use existing status
        Heimdall_Backup_Config__c existingBackup = salesforceService.getObjectBackup(item.name);
        String status;
        if (existingBackup != null) {
            // Preserve existing status (user might have manually set to "No Backup")
            status = existingBackup.getStatus__c();
            log.debug("Preserving existing status for {}: {}", item.name, status);
        } else {
            // New object - set status based on replicateable flag and default list
            if (!item.replicateable) {
                status = Heimdall_Backup_Config__c.NOT_REPLICABLE;
            } else if (DEFAULT_BACKUP_OBJECTS.contains(item.name)) {
                status = Heimdall_Backup_Config__c.BACKUP;
                log.info("Enabling backup by default for: {}", item.name);
            } else {
                status = Heimdall_Backup_Config__c.NO_BACKUP;
            }
        }

        Heimdall_Backup_Config__c objectBackup__c = new Heimdall_Backup_Config__c(item.name, status);
        salesforceService.createOrUpdateObjectBackupRecord(objectBackup__c);

        // IMPORTANT: Return the version from the map which has the correct Id
        // After upsert, the map is updated with the latest version including Id
        Heimdall_Backup_Config__c updatedBackup = salesforceService.getObjectBackup(item.name);
        return updatedBackup != null ? updatedBackup : objectBackup__c;
    }
}
