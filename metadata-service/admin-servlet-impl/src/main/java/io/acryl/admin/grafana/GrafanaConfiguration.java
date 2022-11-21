package io.acryl.admin.grafana;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import lombok.Builder;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
@PropertySource(value = "classpath:/grafana-application.yml", factory = YamlPropertySourceFactory.class)
public class GrafanaConfiguration {
    @Value("${grafana.uri}")
    private String uri;

    @Value("${grafana.token}")
    private String token;

    @Value("${grafana.orgId}")
    private String orgId;

    @Value("${grafana.namespace}")
    private String namespace;

    @Value("${grafana.dashboards}")
    private String dashboards;

    @Bean("grafanaDashboards")
    public Map<String, String> grafanaDashboards() {
        Optional<Map<String, String>> parseOpt = Optional.ofNullable(
                new Gson().fromJson(dashboards, new TypeToken<Map<String, String>>() { }.getType()));
        return parseOpt.orElse(Map.of());
    }

    @Bean("grafanaAllow")
    public Set<String> grafanaAllow(@Qualifier("grafanaDashboards") Map<String, String> dashboards) {
        return ImmutableSet.<String>builder()
                .addAll(dashboards.values().stream().
                        map(url -> url.split("[?]", 2)[0]).collect(Collectors.toList()))
                .add("/public")
                .add("/api/ds")
                .add("/api/dashboards")
                .add("/api/prometheus")
                .add("/api/frontend-metrics")
                .add("/config")
                .add("/rules")
                .build();
    }

    @Bean("grafanaRequiredParameters")
    public List<Map.Entry<String, String[]>> grafanaRequiredParameters() {
        return List.of(
                Map.entry("orgId", new String[]{orgId}),
                Map.entry("var-namespace", new String[]{namespace}),
                Map.entry("kiosk", new String[]{""})
        );
    }

    @Bean("grafanaConfig")
    public Config grafanaConfig() {
        return Config.builder()
                .grafanaUri(URI.create(uri))
                .grafanaToken(token)
                .orgId(orgId)
                .namespace(namespace)
                .build();
    }

    @Builder
    @Getter
    public static class Config {
        private URI grafanaUri;
        private String grafanaToken;
        private String orgId;
        private String namespace;
    }
}