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

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import se.devrandom.heimdall.salesforce.CustomItemReaderWriter;
import se.devrandom.heimdall.salesforce.SalesforceService;
import se.devrandom.heimdall.salesforce.objects.DescribeSObjectResult;
import se.devrandom.heimdall.salesforce.objects.Heimdall_Backup_Config__c;
import se.devrandom.heimdall.salesforce.objects.DescribeGlobalResult;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Configuration
@ConditionalOnProperty(name = "spring.batch.job.enabled", havingValue = "true", matchIfMissing = true)
public class BatchConfiguration {

    private final SalesforceService salesforceService;
    @Autowired
    public BatchConfiguration(SalesforceService salesforceService) {
        this.salesforceService = salesforceService;
    }
    @Bean
    public CustomItemReaderWriter<DescribeGlobalResult> getDescribeGlobalResultIO() {
        return salesforceService.getDescribeGlobalResultIO();
    }
    @Bean
    public CustomItemReaderWriter<Heimdall_Backup_Config__c> getObjectBackupIO() {
        return salesforceService.getObjectBackupIO();
    }

    @Bean
    public CustomItemReaderWriter<DescribeSObjectResult> getDescribeSObjectResultIO() {
        return salesforceService.getDescribeSObjectResultIO();
    }

    @Bean
    public SObjectDescriptionProcessor sObjectDescriptionProcessor() {
        return new SObjectDescriptionProcessor();
    }
    @Bean
    public ObjectBackupProcessor objectBackupProcessor() {
        return new ObjectBackupProcessor();
    }

    @Bean
    public Job extractSObjectDescriptionJob(JobRepository jobRepository,
                                            JobCompletionNotificationListener listener,
                                            Step step1) {
        return new JobBuilder("extractSObjectDescriptionJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(step1)
                .build();
    }

    @Bean
    public CompositeItemProcessor compositeProcessor() {
        List<ItemProcessor> delegates = new ArrayList<>(2);
        delegates.add(sObjectDescriptionProcessor());  // Creates/updates Heimdall_Backup_Config__c records
        delegates.add(objectBackupProcessor());         // Runs backup for Status="Backup" objects with COUNT optimization
        CompositeItemProcessor processor = new CompositeItemProcessor();
        processor.setDelegates(delegates);
        return processor;
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor("heimdall-thread-");
        taskExecutor.setConcurrencyLimit(4); // Begränsar till 4 samtidiga trådar

        return new StepBuilder("step1", jobRepository)
                .<DescribeGlobalResult, DescribeSObjectResult>chunk(200, transactionManager)
                .reader(getDescribeGlobalResultIO())
                .processor(compositeProcessor())
                .writer(getDescribeSObjectResultIO())
                // Lägg till parallellitet här
                .taskExecutor(taskExecutor)
                .throttleLimit(4)
                .build();
    }
}
