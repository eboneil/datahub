package auth.sso;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;

import static auth.AuthUtils.*;
import static auth.ConfigUtil.*;


/**
 * Class responsible for extracting and validating top-level SSO related configurations.
 * TODO: Refactor SsoConfigs to have OidcConfigs and other identity provider specific configs as instance variables.
 * SSoManager should ideally not know about identity provider specific configs.
 */
public class SsoConfigs {

  /**
   * Required configs
   */
  private static final String AUTH_BASE_URL_CONFIG_PATH = "auth.baseUrl";
  private static final String AUTH_BASE_CALLBACK_PATH_CONFIG_PATH = "auth.baseCallbackPath";
  private static final String AUTH_SUCCESS_REDIRECT_PATH_CONFIG_PATH = "auth.successRedirectPath";
  public static final String OIDC_ENABLED_CONFIG_PATH = "auth.oidc.enabled";

  /**
   * Default values
   */
  private static final String DEFAULT_BASE_CALLBACK_PATH = "/callback";
  private static final String DEFAULT_SUCCESS_REDIRECT_PATH = "/";

  private final String _authBaseUrl;
  private final String _authBaseCallbackPath;
  private final String _authSuccessRedirectPath;
  private final Integer _sessionTtlInHours;
  private final Boolean _oidcEnabled;
  private final String _authCookieSameSite;
  private final Boolean _authCookieSecure;

  public SsoConfigs(final com.typesafe.config.Config configs) {
    _authBaseUrl = getRequired(configs, AUTH_BASE_URL_CONFIG_PATH);
    _authBaseCallbackPath = getOptional(
        configs,
        AUTH_BASE_CALLBACK_PATH_CONFIG_PATH,
        DEFAULT_BASE_CALLBACK_PATH);
    _authSuccessRedirectPath = getOptional(
        configs,
        AUTH_SUCCESS_REDIRECT_PATH_CONFIG_PATH,
        DEFAULT_SUCCESS_REDIRECT_PATH);
    _sessionTtlInHours = Integer.parseInt(getOptional(
        configs,
        SESSION_TTL_CONFIG_PATH,
        DEFAULT_SESSION_TTL_HOURS.toString()));
    _oidcEnabled =  configs.hasPath(OIDC_ENABLED_CONFIG_PATH)
        && Boolean.TRUE.equals(
        Boolean.parseBoolean(configs.getString(OIDC_ENABLED_CONFIG_PATH)));
    _authCookieSameSite = getOptional(
        configs,
        AUTH_COOKIE_SAME_SITE,
        DEFAULT_AUTH_COOKIE_SAME_SITE);
    _authCookieSecure = Boolean.parseBoolean(getOptional(
        configs,
        AUTH_COOKIE_SECURE,
        String.valueOf(DEFAULT_AUTH_COOKIE_SECURE)));
  }

  public String getAuthBaseUrl() {
    return _authBaseUrl;
  }

  public String getAuthBaseCallbackPath() {
    return _authBaseCallbackPath;
  }

  public String getAuthSuccessRedirectPath() {
    return _authSuccessRedirectPath;
  }

  public Integer getSessionTtlInHours() {
    return _sessionTtlInHours;
  }

  public String getAuthCookieSameSite() {
    return _authCookieSameSite;
  }

  public boolean getAuthCookieSecure() {
    return _authCookieSecure;
  }

  public Boolean isOidcEnabled() {
    return _oidcEnabled;
  }

  public static class Builder<T extends Builder<T>> {
    private String _authBaseUrl = null;
    private String _authBaseCallbackPath = DEFAULT_BASE_CALLBACK_PATH;
    private String _authSuccessRedirectPath = DEFAULT_SUCCESS_REDIRECT_PATH;
    private Integer _sessionTtlInHours = DEFAULT_SESSION_TTL_HOURS;
    protected Boolean _oidcEnabled = false;
    private final ObjectMapper _objectMapper = new ObjectMapper();
    protected JsonNode jsonNode = null;


    // No need to check if changes are made since this method is only called at start-up.
    public Builder from(final com.typesafe.config.Config configs) {
      if (configs.hasPath(AUTH_BASE_URL_CONFIG_PATH)) {
        _authBaseUrl = configs.getString(AUTH_BASE_URL_CONFIG_PATH);
      }
      if (configs.hasPath(AUTH_BASE_CALLBACK_PATH_CONFIG_PATH)) {
        _authBaseCallbackPath = configs.getString(AUTH_BASE_CALLBACK_PATH_CONFIG_PATH);
      }
      if (configs.hasPath(OIDC_ENABLED_CONFIG_PATH)) {
        _oidcEnabled = Boolean.TRUE.equals(Boolean.parseBoolean(configs.getString(OIDC_ENABLED_CONFIG_PATH)));
      }
      if (configs.hasPath(AUTH_SUCCESS_REDIRECT_PATH_CONFIG_PATH)) {
        _authSuccessRedirectPath = configs.getString(AUTH_SUCCESS_REDIRECT_PATH_CONFIG_PATH);
      }
      if (configs.hasPath(SESSION_TTL_CONFIG_PATH)) {
        _sessionTtlInHours = Integer.parseInt(configs.getString(SESSION_TTL_CONFIG_PATH));
      }
      return this;
    }

    public Builder from(String ssoSettingsJsonStr) {
      try {
        jsonNode = _objectMapper.readTree(ssoSettingsJsonStr);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to parse ssoSettingsJsonStr %s into JSON", ssoSettingsJsonStr));
      }
      if (jsonNode.has(BASE_URL)) {
        _authBaseUrl = jsonNode.get(BASE_URL).asText();
      }
      if (jsonNode.has(OIDC_ENABLED)) {
        _oidcEnabled = jsonNode.get(OIDC_ENABLED).asBoolean();
      }

      return this;
    }

    public SsoConfigs build() {
      Objects.requireNonNull(this._authBaseUrl, "authBaseUrl is required");
      return new SsoConfigs(this);
    }
  }
}
