export default async function (ctx) {
  // ===== LOAD-TEST KNOB =====================================================
  // Number of times the full matched product set is re-read from the DB and its
  // TSV rows appended to the SAME output file. Set back to 1 for the single-pass
  // production export.
  const PASSES = ctx.input?.passes ?? 9;
  // ==========================================================================

  const input = ctx.input || {};
  const bcEnv = input.bcEnv;
  if (!bcEnv) return { status: 'error', message: 'bcEnv is required' };
  const fileName = input.fileName || 'Belami Data';
  const startCursor = input.startCursor ?? 0;
  const endCursor = input.endCursor ?? null; // SHARD upper bound (id <= endCursor)
  const maxProductsToExport = input.maxProductsToExport ?? null;
  const syncJobId = input.syncJobId ?? null;
  const forceRediscover = input.forceRediscover === true;

  // SFTP destination -- TEMP test subfolder while validating at scale.
  const FTP_REMOTE_PATH = '/incoming/test';

  // Streaming knobs. PRODUCT_PAGE is both the keyset page size and the per-chunk
  // batch for the metafield/image/variant joins. Measured: at 200 products the
  // largest per-call result (metafields) is ~1MB, far under the 32MB bridge
  // guard, and round-trip overhead (~600ms/query) dominates -- so a larger page
  // amortizes it. The per-call result guard is the hard backstop; if a
  // pathological chunk trips it, PRODUCT_PAGE must come down (or add adaptive
  // splitting). 4MB flush keeps the isolate well under its 128MB cap.
  const PRODUCT_PAGE = input.productPage ?? 300;
  const FLUSH_CHAR_THRESHOLD = 4 * 1024 * 1024;
  const CHECKPOINT_KEY = 'export-progress';

  // ---- Resilient v1 DB reads (transient retry; deterministic errors not retried) ----
  async function v1Query(sql, params, label, maxAttempts = 3) {
    let lastErr;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const result = await fastn.db.v1.query(sql, params);
        return result.rows;
      } catch (e) {
        lastErr = e;
        if (/per-call limit/i.test(e?.message || '')) break; // deterministic -- do not retry
      }
    }
    throw new Error(`v1 DB query failed [${label}]: ${lastErr?.message || lastErr}`);
  }

  const tableNames = await fastn.envConfig.get('tableNames');
  const bcTables = {
    products: `${tableNames.bigCommerce.products}_${bcEnv}`,
    variants: `${tableNames.bigCommerce.productVariants}_${bcEnv}`,
    metafields: `${tableNames.bigCommerce.metafields}_${bcEnv}`,
    images: `${tableNames.bigCommerce.productImages}_${bcEnv}`,
  };
  const syncJobsTable = `${tableNames.utility.syncJobs}_${bcEnv}`;
  const jobBatchesTable = `${tableNames.utility.jobBatches}_${bcEnv}`;

  // =========================================================================
  // RESUME: a Temporal activity retry re-invokes this SAME executionId, so a run
  // interrupted by a worker restart resumes from its last durable checkpoint
  // instead of restarting from zero. Requires retryPolicy.maxAttempts > 1.
  // =========================================================================
  let resume = null;
  if (ctx.isRetry) {
    try {
      resume = await fastn.checkpoint.get(CHECKPOINT_KEY);
    } catch (_) { resume = null; }
  }
  // without an infra interruption. Removed from the production version.

  // ---- Full vs incremental cutoff (unchanged semantics) ----
  let cutoffDate = null;
  let syncJobInfo = null;
  if (syncJobId && !resume) {
    const syncJobRows = await v1Query(`SELECT * FROM "${syncJobsTable}" WHERE id = $1`, [syncJobId], 'lookup syncJob');
    if (syncJobRows.length === 0) return { status: 'error', message: `syncJobId ${syncJobId} not found in ${syncJobsTable}` };
    const countRows = await v1Query(
      `SELECT COALESCE(SUM(products_synced), 0) AS total FROM "${jobBatchesTable}" WHERE job_id = $1`,
      [syncJobId], 'sum products_synced');
    const totalSynced = Number(countRows[0].total);
    if (totalSynced === 0) return { status: 'not_initiated', reason: 'No products synced for the given syncJobId' };
    cutoffDate = syncJobRows[0].created_on;
    syncJobInfo = { id: syncJobRows[0].id, created_on: syncJobRows[0].created_on, totalSynced };
  } else if (resume) {
    cutoffDate = resume.cutoffDate ?? null;
    syncJobInfo = resume.syncJobInfo ?? null;
  }

  // Keyset predicate builders (replaces OFFSET; index-driven on pkey/id).
  function productPageSql(afterId) {
    const params = [];
    let sql = `SELECT id, data FROM "${bcTables.products}" WHERE sku IS NOT NULL AND sku <> ''`;
    if (cutoffDate) { params.push(cutoffDate); sql += ` AND synced_at >= $${params.length}`; }
    params.push(afterId); sql += ` AND id > $${params.length}`;
    if (endCursor !== null) { params.push(endCursor); sql += ` AND id <= $${params.length}`; }
    sql += ` ORDER BY id LIMIT ${PRODUCT_PAGE}`;
    return { sql, params };
  }

  // ---- Header resolution -------------------------------------------------------------
  // The 21M-row metafields DISTINCT scan is the observed timeout (exec_218a9b50685c).
  // Default: trust the persisted baseline; discover new columns INCREMENTALLY via the
  // (synced_at, resource_type, namespace, key) index. Full rebuild (loose index scan,
  // index-only) only on forceRediscover or when no baseline exists.
  async function fullDiscovery() {
    // Loose index scan (skip-scan) per resource_type using idx (resource_type, namespace, key).
    // Index-only; ~0.8ms per distinct pair vs a 16M-row seq scan.
    const skipSql = `
      WITH RECURSIVE prod AS (
        (SELECT namespace, key FROM "${bcTables.metafields}" WHERE resource_type='product' ORDER BY namespace, key LIMIT 1)
        UNION ALL SELECT n.namespace, n.key FROM prod CROSS JOIN LATERAL (
          SELECT namespace, key FROM "${bcTables.metafields}"
          WHERE resource_type='product' AND (namespace, key) > (prod.namespace, prod.key)
          ORDER BY namespace, key LIMIT 1) n),
      var AS (
        (SELECT namespace, key FROM "${bcTables.metafields}" WHERE resource_type='variant' ORDER BY namespace, key LIMIT 1)
        UNION ALL SELECT n.namespace, n.key FROM var CROSS JOIN LATERAL (
          SELECT namespace, key FROM "${bcTables.metafields}"
          WHERE resource_type='variant' AND (namespace, key) > (var.namespace, var.key)
          ORDER BY namespace, key LIMIT 1) n)
      SELECT namespace, key FROM (
        SELECT namespace, key FROM prod
        UNION SELECT namespace, key FROM var WHERE namespace NOT IN ('Accessories','product_images')
      ) u WHERE namespace NOT IN ('delivery_message') ORDER BY namespace, key`;
    return v1Query(skipSql, [], 'header full discovery (skip-scan)');
  }
  async function incrementalDiscovery(sinceIso) {
    // Bounded by the synced_at index -- only metafields synced since the watermark.
    return v1Query(
      `SELECT DISTINCT namespace, key FROM "${bcTables.metafields}"
       WHERE synced_at >= $1
         AND (resource_type='product' OR (resource_type='variant' AND namespace NOT IN ('Accessories','product_images')))
         AND namespace NOT IN ('delivery_message')
       ORDER BY namespace, key`,
      [sinceIso], 'header incremental discovery');
  }

  let headers, hasNewColumns = false, newColumns = [];
  if (resume) {
    headers = resume.headers; // frozen for the life of the run
  } else {
    const baselineHeader = (await fastn.envConfig.get('feedonomicsFileHeader')) || [];
    const sampleProductRows = await v1Query(`SELECT data FROM "${bcTables.products}" LIMIT 1`, [], 'sample product');
    const sampleVariantRows = await v1Query(`SELECT data FROM "${bcTables.variants}" LIMIT 1`, [], 'sample variant');
    const productFields = Object.keys(sampleProductRows[0].data).map((k) => `product_${k}`);
    const imageFields = ['image_1', 'image_2', 'image_3', 'image_4', 'image_5'];
    const variantFields = Object.keys(sampleVariantRows[0].data).map((k) => `variant_${k}`);

    let metafieldKeyRows;
    const watermark = await fastn.envConfig.get('feedonomicsHeaderWatermark');
    if (baselineHeader.length > 0 && !forceRediscover) {
      // Hot path: never scan 21M rows. Incremental if we have a watermark, else trust baseline.
      metafieldKeyRows = (cutoffDate || watermark)
        ? await incrementalDiscovery(cutoffDate || watermark)
        : [];
    } else {
      metafieldKeyRows = await fullDiscovery();
    }
    const metafieldColumns = metafieldKeyRows.map((r) => `${r.namespace}.${r.key}`);
    const discoveredHeaders = ['resource_type', ...productFields, ...imageFields, ...variantFields, ...metafieldColumns];

    if (baselineHeader.length === 0) {
      headers = discoveredHeaders;
      hasNewColumns = true;
      newColumns = discoveredHeaders;
    } else {
      const baselineSet = new Set(baselineHeader);
      newColumns = discoveredHeaders.filter((h) => !baselineSet.has(h));
      headers = [...baselineHeader, ...newColumns]; // additive only
      hasNewColumns = newColumns.length > 0;
    }
    if (hasNewColumns) await fastn.envConfig.set('feedonomicsFileHeader', headers);
    // Advance the watermark so subsequent runs stay incremental (and the first-ever
    // run with no watermark doesn't force a full scan next time).
    await fastn.envConfig.set('feedonomicsHeaderWatermark', new Date().toISOString());
  }

  // ---- Job row (created once; reused across retries) ----
  let job, localPath, fileKey;
  if (resume) {
    job = { id: resume.jobId, created_at: resume.jobCreatedAt };
    localPath = resume.localPath;
    fileKey = resume.fileKey;
  } else {
    const estimateRows = await v1Query(
      `SELECT count(*)::bigint AS n FROM "${bcTables.products}" WHERE sku IS NOT NULL AND sku <> ''${cutoffDate ? ' AND synced_at >= $1' : ''}${maxProductsToExport ? '' : ''}`,
      cutoffDate ? [cutoffDate] : [], 'estimate product count');
    const matched = Math.min(Number(estimateRows[0].n), maxProductsToExport ?? Infinity);
    const jobRows = await fastn.db.query(
      `INSERT INTO fdx_export_jobs (bc_env, sync_job_id, max_products, status, base_file_name, file_header, has_new_columns, new_columns, total_products, products_processed)
       VALUES ($1,$2,$3,'active',$4,$5,$6,$7,$8,0) RETURNING id, created_at`,
      [bcEnv, syncJobId, maxProductsToExport, fileName, JSON.stringify(headers), hasNewColumns, JSON.stringify(newColumns), matched * PASSES]);
    job = jobRows[0];
    localPath = `feedonomics-exports/job-${job.id}.tsv`;
    await fastn.db.query(`UPDATE fdx_export_jobs SET file_path = $1 WHERE id = $2`, [localPath, job.id]);
  }
  const dateStr = String(job.created_at).split('T')[0];

  try {
    // ---- TSV serialization helpers ----
    function tsvCell(v) { if (v === null || v === undefined) return ''; return String(v).replace(/[\t\r\n]+/g, ' '); }
    function serializeValue(v) {
      if (v === null || v === undefined) return '';
      if (typeof v === 'object') return tsvCell(JSON.stringify(v).slice(0, 50000));
      return tsvCell(String(v).slice(0, 50000));
    }
    const headerLine = headers.join('\t') + '\n';
    const metafieldColumnSet = new Set(headers.filter((h) => !h.startsWith('product_') && !h.startsWith('variant_') && !h.startsWith('image_') && h !== 'resource_type'));
    function buildProductRow(productJson, productImageUrls, productMetas) {
      const row = {};
      for (const header of headers) {
        if (header === 'resource_type') row[header] = 'product';
        else if (header.startsWith('product_')) row[header] = serializeValue(productJson[header.slice(8)]);
        else if (header.startsWith('image_')) { const url = productImageUrls[parseInt(header.slice(6), 10) - 1]; row[header] = url ? tsvCell(url) : ''; }
        else if (header.startsWith('variant_')) row[header] = '';
        else if (metafieldColumnSet.has(header)) row[header] = serializeValue(productMetas[header] ?? '');
      }
      return row;
    }
    function buildVariantRow(productJson, variantJson, variantMetas) {
      const row = {};
      for (const header of headers) {
        if (header === 'resource_type') row[header] = 'variant';
        else if (header.startsWith('product_')) row[header] = serializeValue(productJson[header.slice(8)]);
        else if (header.startsWith('image_')) row[header] = '';
        else if (header.startsWith('variant_')) row[header] = serializeValue(variantJson[header.slice(8)]);
        else if (metafieldColumnSet.has(header)) row[header] = serializeValue(variantMetas[header] ?? '');
      }
      return row;
    }
    function rowToLine(row) { return headers.map((h) => row[h] ?? '').join('\t'); }

    // Fresh run writes the header line and truncates any prior temp file. A resume
    // leaves the existing file (header + already-appended rows) untouched.
    if (!resume) {
      if (await fastn.files.exists(localPath)) await fastn.files.delete(localPath);
      fileKey = await fastn.files.write(localPath, headerLine);
    }

    // ---- Size-flushed buffer; cursor is checkpointed only after a flush lands ----
    let pendingLines = [];
    let pendingCharCount = 0;
    let pendingMaxCursor = resume ? resume.cursor : startCursor;
    let totalRowsWritten = resume ? resume.totalRows : 0;
    let totalBytesWritten = resume ? resume.totalBytes : headerLine.length;
    let productsProcessed = resume ? resume.productsProcessed : 0;

    async function flushPending(force) {
      if (pendingLines.length === 0) return false;
      if (!force && pendingCharCount < FLUSH_CHAR_THRESHOLD) return false;
      const text = pendingLines.join('\n') + '\n';
      await fastn.files.append(localPath, text);
      totalRowsWritten += pendingLines.length;
      totalBytesWritten += text.length;
      pendingLines = [];
      pendingCharCount = 0;
      return true;
    }
    function pushLine(line) { pendingLines.push(line); pendingCharCount += line.length + 1; }

    // Checkpointing and progress tracking are best-effort: a failed checkpoint only
    // disables resume for that increment (the file output is still correct), so it must
    // never fail a multi-hour export. In the Test panel there is no persisted execution
    // row, so checkpoint.set FK-fails and is swallowed; real /execute runs persist fine.
    async function saveCheckpoint(pass, cursor) {
      try {
        await fastn.checkpoint.set(CHECKPOINT_KEY, {
          jobId: job.id, jobCreatedAt: job.created_at, localPath, fileKey, headers,
          cutoffDate, syncJobInfo, pass, cursor,
          productsProcessed, totalRows: totalRowsWritten, totalBytes: totalBytesWritten,
        });
      } catch (e) {
        console.warn(`checkpoint.set failed (resume disabled for this increment): ${e?.message || e}`);
      }
      try {
        await fastn.db.query(`UPDATE fdx_export_jobs SET products_processed = $1, updated_at = now() WHERE id = $2`, [productsProcessed, job.id]);
      } catch (e) {
        console.warn(`job progress update failed: ${e?.message || e}`);
      }
    }

    let firstProcessedId = null, lastProcessedId = null;
    const startPass = resume ? resume.pass : 1;
    for (let pass = startPass; pass <= PASSES; pass++) {
      // Resume mid-pass only for the pass we were interrupted on; later passes start at 0.
      let cursor = (resume && pass === resume.pass) ? resume.cursor : startCursor;
      let processedThisPass = 0;

      for (;;) {
        if (maxProductsToExport && processedThisPass >= maxProductsToExport) break;
        const { sql, params } = productPageSql(cursor);
        const products = await v1Query(sql, params, `products pass ${pass} after ${cursor}`);
        if (products.length === 0) break;
        const chunkIds = products.map((p) => p.id);
        const idsArr = `ANY($1::bigint[])`;

        const images = await v1Query(
          `SELECT product_id, url_tiny FROM (
             SELECT product_id, data->>'url_tiny' AS url_tiny,
                    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY id) AS rn
             FROM "${bcTables.images}" WHERE product_id = ${idsArr}
           ) t WHERE rn <= 5 ORDER BY product_id, rn;`, [chunkIds], `images pass ${pass}`);
        const productMetafields = await v1Query(
          `SELECT resource_id, namespace, key, data->>'value' AS value FROM "${bcTables.metafields}"
           WHERE resource_type = 'product' AND namespace NOT IN ('delivery_message') AND resource_id = ${idsArr}
           ORDER BY resource_id, namespace, key, id;`, [chunkIds], `product metafields pass ${pass}`);
        const variants = await v1Query(
          `SELECT id, product_id, data FROM "${bcTables.variants}" WHERE product_id = ${idsArr} ORDER BY product_id, id;`,
          [chunkIds], `variants pass ${pass}`);

        const productsById = {};
        for (const p of products) productsById[p.id] = p;
        const imageUrlsByProduct = {};
        for (const img of images) (imageUrlsByProduct[img.product_id] ??= []).push(img.url_tiny);
        const productMetafieldsByProduct = {};
        for (const mf of productMetafields) (productMetafieldsByProduct[mf.resource_id] ??= {})[`${mf.namespace}.${mf.key}`] = mf.value;

        const variantIds = variants.map((v) => v.id);
        const variantMetafieldsByVariant = {};
        if (variantIds.length > 0) {
          const variantMetafields = await v1Query(
            `SELECT resource_id, namespace, key, data->>'value' AS value FROM "${bcTables.metafields}"
             WHERE resource_type = 'variant' AND namespace NOT IN ('Accessories','product_images','delivery_message')
               AND resource_id = ${idsArr} ORDER BY resource_id, namespace, key, id;`,
            [variantIds], `variant metafields pass ${pass}`);
          for (const mf of variantMetafields) (variantMetafieldsByVariant[mf.resource_id] ??= {})[`${mf.namespace}.${mf.key}`] = mf.value;
        }
        const variantsByProduct = {};
        for (const v of variants) (variantsByProduct[v.product_id] ??= []).push(v);

        // Emit product row then its variant rows, in id order (stable, resume-safe).
        for (const p of products) {
          const productImageUrls = imageUrlsByProduct[p.id] ?? [];
          const productMetas = productMetafieldsByProduct[p.id] ?? {};
          pushLine(rowToLine(buildProductRow(p.data, productImageUrls, productMetas)));
          const pv = variantsByProduct[p.id] ?? [];
          for (const variant of pv) {
            const variantMetas = variantMetafieldsByVariant[variant.id] ?? {};
            pushLine(rowToLine(buildVariantRow(p.data, variant.data, variantMetas)));
          }
        }

        if (firstProcessedId === null) firstProcessedId = chunkIds[0];
        cursor = chunkIds[chunkIds.length - 1];
        lastProcessedId = cursor;
        pendingMaxCursor = cursor;
        productsProcessed += chunkIds.length;
        processedThisPass += chunkIds.length;

        // Checkpoint only after a flush actually persisted rows to the file, so the
        // checkpointed cursor always reflects what is durably in object storage.
        const flushed = await flushPending(false);
        if (flushed) await saveCheckpoint(pass, pendingMaxCursor);
      }
      // Pass complete: flush the tail and checkpoint the next pass boundary.
      const flushed = await flushPending(true);
      if (flushed || pass < PASSES) await saveCheckpoint(pass + 1, startCursor);
    }
    await flushPending(true);

    // ---- Final delivery: FileRef passthrough (bytes stream storage -> SFTP directly) ----
    // gzipOutput (opt-in): compress the export before SFTP delivery so a large
    // multi-pass file fits a size-constrained destination. Default off keeps the
    // proven plain-.tsv path for normal single-pass exports byte-identical.
    const GZIP = ctx.input?.gzipOutput ?? false;
    let deliverKey = fileKey;
    let deliverName = `${fileName} - ${dateStr}.tsv`;
    let deliverMime = 'text/tab-separated-values';
    if (GZIP) {
      deliverKey = await fastn.files.gzip(localPath, undefined, { level: ctx.input?.gzipLevel ?? 1 });
      deliverName = `${deliverName}.gz`;
      deliverMime = 'application/gzip';
    }
    const remotePath = `${FTP_REMOTE_PATH}/${deliverName}`;
    const uploadRes = await fastn.connector.sftpServer.uploadFile({
      path: remotePath,
      file: { fileId: `fdx-job-${job.id}`, key: deliverKey, bucket: 'fastn-files', size: 0, mime: deliverMime, name: deliverName },
    });
    await fastn.files.delete(localPath);
    if (GZIP) { try { await fastn.files.delete(`${localPath}.gz`); } catch (e) {} }
    const finalFile = { name: deliverName, path: remotePath, size: uploadRes.output?.size ?? totalBytesWritten };

    await fastn.db.query(
      `UPDATE fdx_export_jobs SET status = 'completed', completed_at = now(), updated_at = now(),
        final_files = $2, final_folder_id = $3, products_processed = $4 WHERE id = $1`,
      [job.id, JSON.stringify([finalFile]), FTP_REMOTE_PATH, productsProcessed]);

    return {
      status: 'completed', jobId: job.id, passes: PASSES,
      totalRows: totalRowsWritten, productsProcessed, finalFile, ftpPath: FTP_REMOTE_PATH,
      resumed: !!resume, resumedFromPass: resume?.pass ?? null, resumedFromCursor: resume?.cursor ?? null,
      firstProcessedId, lastProcessedId, syncJobInfo,
    };
  } catch (err) {
    const message = `Job ${job.id} failed: ${err?.message || err}`;
    try {
      await fastn.db.query(`UPDATE fdx_export_jobs SET status = 'failed', updated_at = now(), error_message = $2 WHERE id = $1`, [job.id, message]);
    } catch (_) {}
    throw new Error(message);
  }
}
