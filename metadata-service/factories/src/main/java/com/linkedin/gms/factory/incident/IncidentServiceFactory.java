package com.linkedin.gms.factory.incident;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.service.IncidentService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({SystemAuthenticationFactory.class, RestliEntityClientFactory.class})
public class IncidentServiceFactory {
  @Autowired
  @Qualifier("restliEntityClient")
  private EntityClient _entityClient;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "incidentService")
  @Scope("singleton")
  @Nonnull
  protected IncidentService getInstance() throws Exception {
    return new IncidentService(_entityClient, _authentication);
  }
}
