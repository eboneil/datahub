package com.linkedin.datahub.upgrade.restorebackup;

import com.google.common.collect.ImmutableBiMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.BackupReader;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.BackupReaderArgs;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.EbeanAspectBackupIterator;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.LocalParquetReader;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.ParquetReaderWrapper;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.S3BackupReader;
import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;


public class RestoreStorageStep implements UpgradeStep {

  private static final int REPORT_BATCH_SIZE = 1000;
  private static final int DEFAULT_THREAD_POOL = 4;

  private final EntityService _entityService;
  private final EntityRegistry _entityRegistry;
  private final Map<String, Class<? extends BackupReader<ParquetReaderWrapper>>> _backupReaders;
  private final ExecutorService _fileReaderThreadPool;

  public RestoreStorageStep(final EntityService entityService, final EntityRegistry entityRegistry) {
    _entityService = entityService;
    _entityRegistry = entityRegistry;
    _backupReaders = ImmutableBiMap.of(LocalParquetReader.READER_NAME, LocalParquetReader.class,
        S3BackupReader.READER_NAME, S3BackupReader.class);
    String poolSize = System.getenv(RestoreIndices.READER_POOL_SIZE);
    int intPoolSize;
    try {
      intPoolSize = Integer.parseInt(poolSize);
    } catch (NumberFormatException e) {
      intPoolSize = DEFAULT_THREAD_POOL;
    }
    _fileReaderThreadPool = Executors.newFixedThreadPool(intPoolSize);
  }

  @Override
  public String id() {
    return "RestoreStorageStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      context.report().addLine("Starting backup restore...");
      int numRows = 0;
      Optional<String> backupReaderName = context.parsedArgs().get("BACKUP_READER");
      if (!backupReaderName.isPresent() || !_backupReaders.containsKey(backupReaderName.get())) {
        context.report().addLine("BACKUP_READER is not set or is not valid");
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }

      Class<? extends BackupReader<ParquetReaderWrapper>> clazz = _backupReaders.get(backupReaderName.get());
      List<String> argNames = BackupReaderArgs.getArgNames(clazz);
      List<Optional<String>> args = argNames.stream().map(argName -> context.parsedArgs().get(argName)).collect(
          Collectors.toList());
      BackupReader<ParquetReaderWrapper> backupReader;
      try {
        backupReader = clazz.getConstructor(List.class).newInstance(args);
      } catch (InstantiationException | InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
        context.report().addLine("Invalid BackupReader, not able to construct instance of " + clazz.getSimpleName());
        throw new IllegalArgumentException("Invalid BackupReader: " + clazz.getSimpleName() + ", need to implement proper constructor.");
      }
      EbeanAspectBackupIterator<ParquetReaderWrapper> iterator = backupReader.getBackupIterator(context);
      ParquetReaderWrapper reader;
      List<Future<?>> futureList = new ArrayList<>();
      while ((reader = iterator.getNextReader()) != null) {
        final ParquetReaderWrapper readerRef = reader;
        futureList.add(_fileReaderThreadPool.submit(() -> readerExecutable(readerRef, context)));
      }
      for (Future<?> future : futureList) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          context.report().addLine("Reading interrupted, not able to finish processing.");
          throw new RuntimeException(e);
        }
      }

      context.report().addLine(String.format("Added %d rows to the aspect v2 table", numRows));
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private void readerExecutable(ParquetReaderWrapper reader, UpgradeContext context) {
    EbeanAspectV2 aspect;
    int numRows = 0;
    while ((aspect = reader.next()) != null) {
      numRows++;

      // 1. Extract an Entity type from the entity Urn
      Urn urn;
      try {
        urn = Urn.createFromString(aspect.getKey().getUrn());
      } catch (Exception e) {
        context.report()
            .addLine(
                String.format("Failed to bind Urn with value %s into Urn object: %s", aspect.getKey().getUrn(), e));
        continue;
      }

      // 2. Verify that the entity associated with the aspect is found in the registry.
      final String entityName = urn.getEntityType();
      final EntitySpec entitySpec;
      try {
        entitySpec = _entityRegistry.getEntitySpec(entityName);
      } catch (Exception e) {
        context.report()
            .addLine(String.format("Failed to find Entity with name %s in Entity Registry: %s", entityName, e));
        continue;
      }
      final String aspectName = aspect.getKey().getAspect();

      // 3. Create record from json aspect
      final RecordTemplate aspectRecord;
      try {
        aspectRecord =
            EntityUtils.toAspectRecord(entityName, aspectName, aspect.getMetadata(), _entityRegistry);
      } catch (Exception e) {
        context.report()
            .addLine(String.format("Failed to create aspect record with name %s associated with entity named %s: %s",
                aspectName, entityName, e));
        continue;
      }

      // 4. Verify that the aspect is a valid aspect associated with the entity
      AspectSpec aspectSpec;
      try {
        aspectSpec = entitySpec.getAspectSpec(aspectName);
      } catch (Exception e) {
        context.report()
            .addLine(String.format("Failed to find aspect spec with name %s associated with entity named %s: %s",
                aspectName, entityName, e));
        continue;
      }

      // 5. Write the row back using the EntityService
      boolean emitMae = aspect.getKey().getVersion() == 0L;
      _entityService.updateAspect(urn, entityName, aspectName, aspectSpec, aspectRecord, toAuditStamp(aspect),
          aspect.getKey().getVersion(), emitMae);

      if (numRows % REPORT_BATCH_SIZE == 0) {
        context.report().addLine(String.format("Successfully inserted %d rows", numRows));
      }
    }
  }

  private AuditStamp toAuditStamp(final EbeanAspectV2 aspect) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(aspect.getCreatedOn().getTime());

    try {
      auditStamp.setActor(new Urn(aspect.getCreatedBy()));
      if (aspect.getCreatedFor() != null) {
        auditStamp.setImpersonator(new Urn(aspect.getCreatedFor()));
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return auditStamp;
  }
}
