package com.linkedin.datahub.upgrade.restoreaspect;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.EbeanAspectBackupIterator;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.ParquetReaderWrapper;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.S3BackupReader;
import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;


public class RestoreAspectStep implements UpgradeStep {

  private final EntityService _entityService;
  private final EntityRegistry _entityRegistry;
  private final ExecutorService _fileReaderThreadPool;

  private static final int DEFAULT_THREAD_POOL = 4;

  public RestoreAspectStep(final EntityService entityService, final EntityRegistry entityRegistry) {
    _entityService = entityService;
    _entityRegistry = entityRegistry;
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
    return "RestoreAspectStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      context.report().addLine("Starting aspect restore...");

      Optional<String> urnToRestore = context.parsedArgs().get("URN");
      Optional<String> aspectToRestore = context.parsedArgs().get("ASPECT_NAME");
      Optional<String> bucket = context.parsedArgs().get("BACKUP_S3_BUCKET");
      Optional<String> path = context.parsedArgs().get("BACKUP_S3_PATH");

      if (!urnToRestore.isPresent() || !aspectToRestore.isPresent() || !bucket.isPresent() || !path.isPresent()) {
        context.report().addLine("Missing required arguments. This upgrade requires URN, ASPECT_NAME, BACKUP_S3_BUCKET, BACKUP_S3_PATH");
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }

      Optional<String> s3Region = context.parsedArgs().get(S3BackupReader.S3_REGION);
      EbeanAspectBackupIterator<ParquetReaderWrapper> iterator =
          new S3BackupReader(Collections.singletonList(s3Region.orElse(null))).getBackupIterator(context);
      ParquetReaderWrapper reader;
      List<Future<UpgradeStepResult>> futureList = new ArrayList<>();
      while ((reader = iterator.getNextReader()) != null) {
        final ParquetReaderWrapper readerRef = reader;
        futureList.add(_fileReaderThreadPool.submit(() -> readerExecutable(readerRef, context, urnToRestore, aspectToRestore)));
      }

      for (Future<UpgradeStepResult> future : futureList) {
        try {
          UpgradeStepResult stepResult = future.get();
          if (UpgradeStepResult.Result.FAILED.equals(stepResult.result())) {
            return stepResult;
          }
        } catch (InterruptedException | ExecutionException e) {
          context.report().addLine("Interrupted during read, unable to finish execution.");
          return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
        } finally {
          _fileReaderThreadPool.shutdown();
        }
      }

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private UpgradeStepResult readerExecutable(ParquetReaderWrapper reader, UpgradeContext context,
      Optional<String> urnToRestore, Optional<String> aspectToRestore) {
    EbeanAspectV2 aspect;
    while ((aspect = reader.next()) != null) {

      Urn urn;
      try {
        urn = Urn.createFromString(aspect.getKey().getUrn());
      } catch (Exception e) {
        context.report()
            .addLine(
                String.format("Failed to bind Urn with value %s into Urn object: %s", aspect.getKey().getUrn(), e));
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }

      final String aspectName = aspect.getKey().getAspect();

      // If we've found the matching URN + aspect, proceed.
      if (urn.toString().equals(urnToRestore.get()) && aspectName.equals(aspectToRestore.get())) {

        // 2. Verify that the entity associated with the aspect is found in the registry.
        final String entityName = urn.getEntityType();
        final EntitySpec entitySpec;
        try {
          entitySpec = _entityRegistry.getEntitySpec(entityName);
        } catch (Exception e) {
          context.report()
              .addLine(String.format("Failed to find Entity with name %s in Entity Registry: %s", entityName, e));
          return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
        }

        // 3. Create record from json aspect
        final RecordTemplate aspectRecord =
            EntityUtils.toAspectRecord(entityName, aspectName, aspect.getMetadata(), _entityRegistry);

        // 4. Verify that the aspect is a valid aspect associated with the entity
        AspectSpec aspectSpec;
        try {
          aspectSpec = entitySpec.getAspectSpec(aspectName);
        } catch (Exception e) {
          context.report()
              .addLine(String.format("Failed to find aspect spec with name %s associated with entity named %s: %s",
                  aspectName, entityName, e));
          return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
        }

        // 5. Write the row back using the EntityService
        context.report().addLine(String.format("Found aspect to restore with version %s. Restoring..", aspect.getKey().getVersion()));

        boolean emitMae = aspect.getKey().getVersion() == 0L;
        _entityService.updateAspect(
            urn,
            entityName,
            aspectName,
            aspectSpec,
            aspectRecord,
            toAuditStamp(aspect),
            aspect.getKey().getVersion(),
            emitMae);
      }
    }
    return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
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