package com.datahub.authentication.authenticator;

import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticatorContext;
import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.testng.annotations.Test;

import static com.datahub.authentication.AuthenticationConstants.*;
import static com.datahub.authentication.authenticator.DataHubTokenAuthenticator.*;
import static org.testng.Assert.*;


public class LegacyTokenAuthenticatorTest {

  private static final String TEST_SIGNING_KEY = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=";

  @Test
  public void testInit() {
    final LegacyTokenAuthenticator authenticator = new LegacyTokenAuthenticator();
    assertThrows(() -> authenticator.init(null));
    assertThrows(() -> authenticator.init(Collections.emptyMap()));

    // Correct configs provided.
    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY));
  }

  @Test
  public void testAuthenticateFailureMissingAuthorizationHeader() {
    final LegacyTokenAuthenticator authenticator = new LegacyTokenAuthenticator();
    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY));

    final AuthenticatorContext context = new AuthenticatorContext(Collections.emptyMap());
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateFailureMissingBearerCredentials() {
    final LegacyTokenAuthenticator authenticator = new LegacyTokenAuthenticator();
    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY));

    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(AUTHORIZATION_HEADER_NAME, "Basic username:password")
    );
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateFailureInvalidToken() {
    final LegacyTokenAuthenticator authenticator = new LegacyTokenAuthenticator();
    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY));

    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(AUTHORIZATION_HEADER_NAME, "Bearer someRandomToken")
    );
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateSuccess() throws Exception {
    final LegacyTokenAuthenticator authenticator = new LegacyTokenAuthenticator();
    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY));

    final JwtBuilder builder = Jwts.builder()
        .setIssuer("admin.acryl.io")
        .setExpiration(new Date(System.currentTimeMillis() + 10000))
        .setId(UUID.randomUUID().toString())
        .setSubject("organization");
    byte [] apiKeySecretBytes = TEST_SIGNING_KEY.getBytes(StandardCharsets.UTF_8);
    final Key signingKey = new SecretKeySpec(apiKeySecretBytes, SignatureAlgorithm.HS256.getJcaName());
    final String validToken = builder.signWith(signingKey, SignatureAlgorithm.HS256).compact();

    final String authorizationHeaderValue = String.format("Bearer %s", validToken);
    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(AUTHORIZATION_HEADER_NAME, authorizationHeaderValue)
    );

    final Authentication authentication = authenticator.authenticate(context);

    // Validate the resulting authentication object
    assertNotNull(authentication);
    assertEquals(authentication.getActor().getType(), ActorType.USER);
    assertEquals(authentication.getActor().getId(), "admin");
    assertEquals(authentication.getCredentials(), authorizationHeaderValue);
  }
}
