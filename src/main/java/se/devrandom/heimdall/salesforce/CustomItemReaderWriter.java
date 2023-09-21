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
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import se.devrandom.heimdall.salesforce.objects.Heimdall_Backup_Config__c;

import java.util.ArrayList;
import java.util.List;

public class CustomItemReaderWriter<T> implements ItemReader<T>, ItemWriter<T> {

    private static final Logger log = LoggerFactory.getLogger(CustomItemReaderWriter.class);
    private final List<T> items;
    private final List<T> processedItems;
    private final SalesforceService salesforceService;
    public CustomItemReaderWriter(List<T> items, SalesforceService salesforceService) {
        this.items = items;
        this.processedItems = new ArrayList<T>();
        this.salesforceService = salesforceService;
    }

    @Override
    public synchronized T read() {
        if (!items.isEmpty()) {
            return items.remove(0);
        }
        return null;
    }

    @Override
    public void write(Chunk<? extends T> chunk) throws Exception {
        // Collect all Heimdall_Backup_Config__c records for batch upsert
        List<Heimdall_Backup_Config__c> configsToUpsert = new ArrayList<>();

        for(T c : chunk) {
            if(c instanceof Heimdall_Backup_Config__c objectBackup__c) {
                // Collect all configs - processor has already prepared them
                configsToUpsert.add(objectBackup__c);
            }
            this.processedItems.add(c);
        }

        // Batch upsert all collected configs
        if (!configsToUpsert.isEmpty()) {
            log.info("Batch upserting {} configs", configsToUpsert.size());
            salesforceService.batchUpsertHeimdall_Backup_Config__c(configsToUpsert);
        }
    }

    public List<T> getProcessedItems() {
        return processedItems;
    }
}
