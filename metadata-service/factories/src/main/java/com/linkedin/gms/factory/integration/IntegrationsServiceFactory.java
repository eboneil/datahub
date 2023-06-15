package com.linkedin.gms.factory.integration;

import com.datahub.authentication.Authentication;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.config.IntegrationsServiceConfiguration;
import com.linkedin.metadata.integration.IntegrationsService;
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
@Import({SystemAuthenticationFactory.class})
public class IntegrationsServiceFactory {

  @Autowired
  private ConfigurationProvider _configProvider;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "integrationsService")
  @Scope("singleton")
  @Nonnull
  protected IntegrationsService getInstance() throws Exception {
    final IntegrationsServiceConfiguration config = _configProvider.getIntegrationsService();
    return new IntegrationsService(
      config.host,
      config.port,
      config.useSsl,
      _authentication
    );
  }
}
