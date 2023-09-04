package com.linkedin.metadata.models.registry;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


public interface AspectRetriever {

  RecordTemplate getLatestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName);


}
