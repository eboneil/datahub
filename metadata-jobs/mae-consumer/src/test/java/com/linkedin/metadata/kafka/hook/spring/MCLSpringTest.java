package com.linkedin.metadata.kafka.hook.spring;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.kafka.MetadataChangeLogProcessor;
import com.linkedin.metadata.kafka.hook.UpdateIndicesHook;
import com.linkedin.metadata.kafka.hook.assertion.AssertionsSummaryHook;
import com.linkedin.metadata.kafka.hook.event.EntityChangeEventGeneratorHook;
import com.linkedin.metadata.kafka.hook.incident.IncidentsSummaryHook;
import com.linkedin.metadata.kafka.hook.ingestion.IngestionSchedulerHook;
import com.linkedin.metadata.kafka.hook.notification.NotificationGeneratorHook;
import com.linkedin.metadata.kafka.hook.siblings.SiblingAssociationHook;
import com.linkedin.metadata.kafka.hook.test.MetadataTestHook;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


@SpringBootTest(classes = {
    MCLSpringTestConfiguration.class, ConfigurationProvider.class
  },
    properties = {
      "ingestionScheduler.enabled=false",
      "configEntityRegistry.path=../../metadata-jobs/mae-consumer/src/test/resources/test-entity-registry.yml",
      "kafka.schemaRegistry.type=INTERNAL"
  })
@TestPropertySource(locations = "classpath:/application.yml", properties = {
    "MCL_CONSUMER_ENABLED=true"
})
@EnableAutoConfiguration(exclude = {CassandraAutoConfiguration.class})
public class MCLSpringTest extends AbstractTestNGSpringContextTests {

  @Test
  public void testHooks() {
    MetadataChangeLogProcessor metadataChangeLogProcessor = applicationContext.getBean(MetadataChangeLogProcessor.class);
    assertTrue(metadataChangeLogProcessor.getHooks().stream().noneMatch(hook -> hook instanceof IngestionSchedulerHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof UpdateIndicesHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof SiblingAssociationHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof EntityChangeEventGeneratorHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof NotificationGeneratorHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof IncidentsSummaryHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof MetadataTestHook));
    assertTrue(metadataChangeLogProcessor.getHooks().stream().anyMatch(hook -> hook instanceof AssertionsSummaryHook));
  }
}
