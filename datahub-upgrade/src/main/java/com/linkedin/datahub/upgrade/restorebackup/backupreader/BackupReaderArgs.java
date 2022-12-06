package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Retains a map of what arguments are passed in to a backup reader
 */
public final class BackupReaderArgs {
  private BackupReaderArgs() {

  }

  private static final Map<Class<? extends BackupReader>, List<String>> ARGS_MAP;

  static {
    ARGS_MAP = new HashMap<>();
    ARGS_MAP.put(LocalParquetReader.class, LocalParquetReader.argNames());
<<<<<<< HEAD
    ARGS_MAP.put(S3BackupReader.class, S3BackupReader.argNames());
=======
>>>>>>> oss_master
  }

  public static List<String> getArgNames(Class<? extends BackupReader> clazz) {
    return ARGS_MAP.get(clazz);
  }
}
