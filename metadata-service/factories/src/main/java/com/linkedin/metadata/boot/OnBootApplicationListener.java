package com.linkedin.metadata.boot;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;

import com.linkedin.metadata.version.GitVersion;
import io.sentry.Sentry;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;


/**
 * Responsible for coordinating starting steps that happen before the application starts up.
 */
@Slf4j
@Component
public class OnBootApplicationListener {

  private static final String ROOT_WEB_APPLICATION_CONTEXT_ID = String.format("%s:", WebApplicationContext.class.getName());

  private final CloseableHttpClient httpClient = HttpClients.createDefault();

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Autowired
  @Qualifier("bootstrapManager")
  private BootstrapManager _bootstrapManager;

  @Autowired
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  @Value("${sentry.enabled}")
  private Boolean sentryEnabled;

  @Value("${sentry.dsn}")
  private String sentryDsn;

  @Value("${sentry.env}")
  private String sentryEnv;

  @Value("${sentry.debug}")
  private Boolean sentryDebug;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider provider;


  @EventListener(ContextRefreshedEvent.class)
  public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
    log.warn("OnBootApplicationListener context refreshed! {} event: {}",
        ROOT_WEB_APPLICATION_CONTEXT_ID.equals(event.getApplicationContext().getId()), event);
    if (ROOT_WEB_APPLICATION_CONTEXT_ID.equals(event.getApplicationContext().getId())) {
      if (sentryEnabled) {
        Sentry.init(options -> {
          options.setDsn(sentryDsn);
          options.setRelease(gitVersion.getVersion());
          options.setEnvironment(sentryEnv);
          options.setTracesSampleRate(0.0);
          options.setDebug(sentryDebug);
        });
        if (sentryDebug) {
          try {
            throw new Exception("This is a test.");
          } catch (Exception e) {
            Sentry.captureException(e);
          }
        }
      }
      executorService.submit(isSchemaRegistryAPIServeletReady());
    }
  }

  public Runnable isSchemaRegistryAPIServeletReady() {
    return () -> {
        final HttpGet request = new HttpGet(provider.getKafka().getSchemaRegistry().getUrl());
        int timeouts = 30;
        boolean openAPIServeletReady = false;
        while (!openAPIServeletReady && timeouts > 0) {
          try {
            log.info("Sleeping for 1 second");
            Thread.sleep(1000);
            StatusLine statusLine = httpClient.execute(request).getStatusLine();
            if (statusLine.getStatusCode() == HttpStatus.SC_OK) {
              log.info("Connected!");
              openAPIServeletReady = true;
            }
          } catch (IOException | InterruptedException e) {
            log.info("Failed to connect to open servlet: {}", e.getMessage());
          }
          timeouts--;
        }
        if (!openAPIServeletReady) {
          log.error("Failed to bootstrap DataHub, OpenAPI servlet was not ready after 30 seconds");
          System.exit(1);
        } else {
        _bootstrapManager.start();
        }
    };
  }
}
