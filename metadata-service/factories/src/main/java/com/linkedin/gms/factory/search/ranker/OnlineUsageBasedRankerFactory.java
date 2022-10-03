package com.linkedin.gms.factory.search.ranker;

import com.linkedin.gms.factory.search.features.GraphBasedFeatureFactory;
import com.linkedin.gms.factory.search.features.UsageFeatureFactory;
import com.linkedin.metadata.search.features.UsageFeature;
import com.linkedin.metadata.search.ranker.OnlineUsageBasedRanker;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({UsageFeatureFactory.class, GraphBasedFeatureFactory.class})
public class OnlineUsageBasedRankerFactory {
  @Autowired
  private UsageFeature usageFeature;

  @Bean(name = "usageBasedRanker")
  @Nonnull
  protected OnlineUsageBasedRanker getInstance() {
    return new OnlineUsageBasedRanker(usageFeature);
  }
}
