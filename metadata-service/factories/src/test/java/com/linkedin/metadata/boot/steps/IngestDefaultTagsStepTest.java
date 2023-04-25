package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.tag.TagProperties;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;


/**
 * Test the behavior of IngestDefaultTagsStep.
 *
 */
public class IngestDefaultTagsStepTest {

  private static final Urn TEST_TAG_URN = UrnUtils.getUrn("urn:li:tag:test");

  @Test
  public void testExecuteValidTagsNoExistingTags() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    configureEntityServiceMock(entityService, null);

    final IngestDefaultTagsStep step = new IngestDefaultTagsStep(entityService, "/boot/test_tags.json");

    step.execute();

    TagProperties expectedResult = new TagProperties();
    expectedResult.setName("Test Tag");

    Mockito.verify(entityService, times(1)).ingestProposal(
        Mockito.eq(buildCreateTagProposal(expectedResult)),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );
  }

  @Test
  public void testExecuteValidSettingsExistingSettings() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    configureEntityServiceMock(entityService, new TagProperties().setName("Other name"));

    final IngestDefaultTagsStep step = new IngestDefaultTagsStep(entityService, "/boot/test_tags.json");

    step.execute();

    Mockito.verify(entityService, times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );
  }

  @Test
  public void testExecuteInvalidJsonSettings() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    configureEntityServiceMock(entityService, null);
    final IngestDefaultTagsStep step = new IngestDefaultTagsStep(entityService, "/boot/test_tags_invalid_json.json");
    Assert.assertThrows(RuntimeException.class, step::execute);
    // Verify no interactions
    verifyNoInteractions(entityService);
  }

  @Test
  public void testExecuteInvalidModelSettings() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    configureEntityServiceMock(entityService, null);
    final IngestDefaultTagsStep step = new IngestDefaultTagsStep(entityService, "/boot/test_tags_invalid_model.json");
    Assert.assertThrows(RuntimeException.class, step::execute);
  }

  private static void configureEntityServiceMock(final EntityService mockService, final TagProperties props) {
    Mockito.when(mockService.getLatestAspect(
        Mockito.eq(TEST_TAG_URN),
        Mockito.eq(TAG_PROPERTIES_ASPECT_NAME)
    )).thenReturn(props);
  }

  private static MetadataChangeProposal buildCreateTagProposal(final TagProperties props) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_TAG_URN);
    mcp.setEntityType(TAG_ENTITY_NAME);
    mcp.setAspectName(TAG_PROPERTIES_ASPECT_NAME);
    mcp.setAspect(GenericRecordUtils.serializeAspect(props));
    mcp.setChangeType(ChangeType.UPSERT);
    return mcp;
  }
}
