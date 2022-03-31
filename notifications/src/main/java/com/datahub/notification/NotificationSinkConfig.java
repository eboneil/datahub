package com.datahub.notification;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;


@Data
@AllArgsConstructor
@Getter
public class NotificationSinkConfig {
  /**
   * Static configuration for a notification sink provided via application.yaml at
   * boot time.
   */
  private final Map<String, Object> staticConfig;

  /**
   * Settings provider, which is responsible for resolving platform settings.
   */
  private final SettingsProvider settingsProvider;

  /**
   * User provider, which is responsible for resolving user to their contact info attributes.
   */
  private final UserProvider userProvider;
}
