export default async function (ctx) {
  const input = ctx.input || {};
  const bcEnv = input.bcEnv;
  if (!bcEnv) return { status: 'error', message: 'bcEnv is required' };
  const fileName = input.fileName || 'Belami Data';
  const startOffset = input.startOffset || 0;
  const maxProductsToExport = input.maxProductsToExport ?? null;
  const syncJobId = input.syncJobId ?? null;

  // SFTP destination -- TEMP: using a safe test subfolder while validating at scale.
  // Real production path (from v1 config, matches Feedonomics' expected drop location) is
  // '/incoming/Product Catalog/Delta' -- revert FTP_REMOTE_PATH to that once testing is done.
  const FTP_REMOTE_PATH = '/incoming/test';

  // ---- Resilient v1 DB reads -----------------------------------------------------------------
  // Retries transient failures (e.g. "Connection terminated due to connection timeout").
  // Deterministic failures are NOT retried: the platform's per-call result-size guard
  // ("over the ... per-call limit") returns the same answer every time, so retrying it
  // just burns two more 50MB transfers before failing anyway.
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
  const baselineHeader = (await fastn.envConfig.get('feedonomicsFileHeader')) || [];

  const bcTables = {
    products: `${tableNames.bigCommerce.products}_${bcEnv}`,
    variants: `${tableNames.bigCommerce.productVariants}_${bcEnv}`,
    metafields: `${tableNames.bigCommerce.metafields}_${bcEnv}`,
    images: `${tableNames.bigCommerce.productImages}_${bcEnv}`,
  };
  const syncJobsTable = `${tableNames.utility.syncJobs}_${bcEnv}`;
  const jobBatchesTable = `${tableNames.utility.jobBatches}_${bcEnv}`;

  // ---- Full vs incremental ----
  let cutoffDate = null;
  let syncJobInfo = null;
  if (syncJobId) {
    const syncJobRows = await v1Query(`SELECT * FROM "${syncJobsTable}" WHERE id = $1`, [syncJobId], 'lookup syncJob');
    if (syncJobRows.length === 0) return { status: 'error', message: `syncJobId ${syncJobId} not found in ${syncJobsTable}` };
    const countRows = await v1Query(
      `SELECT COALESCE(SUM(products_synced), 0) AS total FROM "${jobBatchesTable}" WHERE job_id = $1`,
      [syncJobId],
      'sum products_synced'
    );
    const totalSynced = Number(countRows[0].total);
    if (totalSynced === 0) return { status: 'not_initiated', reason: 'No products synced for the given syncJobId' };
    cutoffDate = syncJobRows[0].created_on;
    syncJobInfo = { id: syncJobRows[0].id, created_on: syncJobRows[0].created_on, totalSynced };
  }

  // ---- Product selection (ids only) ----
  let productQuery = `SELECT p.id AS product_id FROM "${bcTables.products}" p WHERE p.sku IS NOT NULL AND p.sku <> ''`;
  const productParams = [];
  if (cutoffDate) {
    productParams.push(cutoffDate);
    productQuery += ` AND p.synced_at >= $${productParams.length}`;
  }
  productQuery += ` ORDER BY p.id`;
  if (maxProductsToExport) {
    productParams.push(maxProductsToExport);
    productQuery += ` LIMIT $${productParams.length}`;
  }
  productParams.push(startOffset);
  productQuery += ` OFFSET $${productParams.length}`;

  const productRows = await v1Query(productQuery, productParams, 'select product ids');
  if (productRows.length === 0) return { status: 'not_initiated', reason: 'No products matched the export criteria' };
  const productIds = productRows.map((r) => r.product_id);

  // ---- Header discovery (identical logic -- additive only) ----
  let mfQuery = `
    SELECT DISTINCT namespace, key FROM "${bcTables.metafields}"
    WHERE (resource_type = 'product' OR (resource_type = 'variant' AND namespace NOT IN ('Accessories', 'product_images')))
      AND namespace NOT IN ('delivery_message')`;
  const mfParams = [];
  if (cutoffDate) {
    mfParams.push(cutoffDate);
    mfQuery += ` AND synced_at >= $${mfParams.length}`;
  }
  mfQuery += ` ORDER BY namespace, key;`;

  const metafieldKeyRows = await v1Query(mfQuery, mfParams, 'select metafield keys');
  const sampleProductRows = await v1Query(`SELECT data FROM "${bcTables.products}" LIMIT 1`, [], 'sample product');
  const sampleVariantRows = await v1Query(`SELECT data FROM "${bcTables.variants}" LIMIT 1`, [], 'sample variant');
  const sampleProduct = sampleProductRows[0];
  const sampleVariant = sampleVariantRows[0];

  const productFields = Object.keys(sampleProduct.data).map((k) => `product_${k}`);
  const imageFields = ['image_1', 'image_2', 'image_3', 'image_4', 'image_5'];
  const metafieldColumns = metafieldKeyRows.map((r) => `${r.namespace}.${r.key}`);
  const variantFields = Object.keys(sampleVariant.data).map((k) => `variant_${k}`);
  const discoveredHeaders = ['resource_type', ...productFields, ...imageFields, ...variantFields, ...metafieldColumns];

  let headers, hasNewColumns = false, newColumns = [];
  if (baselineHeader.length === 0) {
    headers = discoveredHeaders;
  } else {
    const baselineSet = new Set(baselineHeader);
    newColumns = discoveredHeaders.filter((h) => !baselineSet.has(h));
    headers = [...baselineHeader, ...newColumns]; // additive only -- never drop existing baseline columns
    hasNewColumns = newColumns.length > 0;
  }
  if (hasNewColumns) {
    await fastn.envConfig.set('feedonomicsFileHeader', headers);
  }

  // ---- Create job row ----
  const jobRows = await fastn.db.query(
    `INSERT INTO fdx_export_jobs (bc_env, sync_job_id, max_products, status, base_file_name, file_header, has_new_columns, new_columns, total_products, products_processed)
     VALUES ($1,$2,$3,'active',$4,$5,$6,$7,$8,0) RETURNING id, created_at`,
    [bcEnv, syncJobId, maxProductsToExport, fileName, JSON.stringify(headers), hasNewColumns, JSON.stringify(newColumns), productIds.length]
  );
  const job = jobRows[0];
  const dateStr = String(job.created_at).split('T')[0];
  const localPath = `feedonomics-exports/job-${job.id}.tsv`;
  await fastn.db.query(`UPDATE fdx_export_jobs SET file_path = $1 WHERE id = $2`, [localPath, job.id]);

  // ---- Everything from here on is wrapped: any unrecoverable error marks the job row 'failed'. ----
  try {
    // ---- TSV serialization helpers ----
    function tsvCell(v) {
      if (v === null || v === undefined) return '';
      return String(v).replace(/[\t\r\n]+/g, ' ');
    }
    function serializeValue(v) {
      if (v === null || v === undefined) return '';
      if (typeof v === 'object') return tsvCell(JSON.stringify(v).slice(0, 50000));
      return tsvCell(String(v).slice(0, 50000));
    }

    const headerLine = headers.join('\t') + '\n';

    if (await fastn.files.exists(localPath)) await fastn.files.delete(localPath);
    const fileKey = await fastn.files.write(localPath, headerLine);

    const metafieldColumnSet = new Set(
      headers.filter((h) => !h.startsWith('product_') && !h.startsWith('variant_') && !h.startsWith('image_') && h !== 'resource_type')
    );
    function buildProductRow(productJson, productImageUrls, productMetas) {
      const row = {};
      for (const header of headers) {
        if (header === 'resource_type') row[header] = 'product';
        else if (header.startsWith('product_')) row[header] = serializeValue(productJson[header.slice(8)]);
        else if (header.startsWith('image_')) {
          const url = productImageUrls[parseInt(header.slice(6), 10) - 1];
          row[header] = url ? tsvCell(url) : '';
        } else if (header.startsWith('variant_')) row[header] = '';
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
    function rowToLine(row) {
      return headers.map((h) => row[h] ?? '').join('\t');
    }

    // ---- Small in-memory buffer, flushed by SIZE ----
    // 4MB: an append transfers the joined string across the sandbox bridge, which
    // transiently costs ~3x its size inside the 128MB isolate. 16MB was flirting
    // with the ceiling on top of chunk data; 4MB keeps worst-case comfortable.
    const FLUSH_CHAR_THRESHOLD = 4 * 1024 * 1024;
    let pendingLines = [];
    let pendingCharCount = 0;
    let totalRowsWritten = 0;
    let totalBytesWritten = headerLine.length;
    async function flushPending(force) {
      if (pendingLines.length === 0) return;
      if (force || pendingCharCount >= FLUSH_CHAR_THRESHOLD) {
        const text = pendingLines.join('\n') + '\n';
        await fastn.files.append(localPath, text);
        totalRowsWritten += pendingLines.length;
        totalBytesWritten += text.length;
        pendingLines = [];
        pendingCharCount = 0;
      }
    }
    function pushLine(line) {
      pendingLines.push(line);
      pendingCharCount += line.length + 1;
    }

    // ---- Process ALL matched products sequentially (one execution, start to finish) ----
    // PRODUCT_CHUNK 50: safe now that every per-chunk query below is column-trimmed
    // and the images fetch is capped at 5 rows/product in SQL. Bigger chunks matter
    // for the full catalog: 220k products at chunk 10 was on pace to exceed the
    // 6h long-tier ceiling; at 50 with smaller payloads it fits comfortably.
    const PRODUCT_CHUNK = 50;
    const VARIANT_CHUNK = 50;
    let productsProcessed = 0;

    for (let i = 0; i < productIds.length; i += PRODUCT_CHUNK) {
      const chunkIds = productIds.slice(i, i + PRODUCT_CHUNK);
      const chunkLabel = `chunk ${i}-${i + chunkIds.length}`;

      // Only id + data are used downstream (was SELECT * with synced_at/channel_ids dead weight).
      const products = await v1Query(
        `SELECT id, data FROM "${bcTables.products}" WHERE id = ANY($1::bigint[]) ORDER BY id;`,
        [chunkIds], `products ${chunkLabel}`);

      // The file uses at most 5 image URLs per product (image_1..image_5, url_tiny only).
      // Cap in SQL: some products carry tens of thousands of image rows -- fetching them
      // all returned ~51MB for a 10-product chunk and tripped the per-call limit.
      const images = await v1Query(
        `SELECT product_id, url_tiny FROM (
           SELECT product_id, data->>'url_tiny' AS url_tiny,
                  ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY id) AS rn
           FROM "${bcTables.images}"
           WHERE product_id = ANY($1::bigint[])
         ) t WHERE rn <= 5 ORDER BY product_id, rn;`,
        [chunkIds], `images ${chunkLabel}`);

      // Only namespace.key -> value is used; delivery_message is not in the header,
      // so its (very large JSON) values were fetched and thrown away.
      const productMetafields = await v1Query(
        `SELECT resource_id, namespace, key, data->>'value' AS value
         FROM "${bcTables.metafields}"
         WHERE resource_type = 'product' AND namespace NOT IN ('delivery_message')
           AND resource_id = ANY($1::bigint[])
         ORDER BY resource_id, namespace, key, id;`,
        [chunkIds], `product metafields ${chunkLabel}`);

      const variants = await v1Query(
        `SELECT id, product_id, data FROM "${bcTables.variants}" WHERE product_id = ANY($1::bigint[]) ORDER BY product_id, id;`,
        [chunkIds], `variants ${chunkLabel}`);

      const productsById = {};
      for (const p of products) productsById[p.id] = p;
      const imageUrlsByProduct = {};
      for (const img of images) (imageUrlsByProduct[img.product_id] ??= []).push(img.url_tiny);
      const productMetafieldsByProduct = {};
      for (const mf of productMetafields) (productMetafieldsByProduct[mf.resource_id] ??= {})[`${mf.namespace}.${mf.key}`] = mf.value;

      const seenProductIds = new Set();
      const vBatches = [];
      for (let j = 0; j < variants.length; j += VARIANT_CHUNK) {
        const batchVariants = variants.slice(j, j + VARIANT_CHUNK);
        const newProductIds = [];
        for (const v of batchVariants) {
          if (!seenProductIds.has(v.product_id)) {
            newProductIds.push(v.product_id);
            seenProductIds.add(v.product_id);
          }
        }
        vBatches.push({ variants: batchVariants, newProductIds });
      }
      const noVariantProductIds = products.filter((p) => !seenProductIds.has(p.id)).map((p) => p.id);
      if (noVariantProductIds.length > 0) {
        if (vBatches.length === 0) vBatches.push({ variants: [], newProductIds: noVariantProductIds });
        else vBatches[vBatches.length - 1].newProductIds.push(...noVariantProductIds);
      }

      for (const vBatch of vBatches) {
        const batchVariants = vBatch.variants;
        const newProductIds = new Set(vBatch.newProductIds);
        const variantMetafieldsByVariant = {};
        if (batchVariants.length > 0) {
          const variantIds = batchVariants.map((v) => v.id);
          // delivery_message excluded here too: it was the bulk of the variant-metafield
          // payload (per-warehouse JSON blobs) and never appears in the header.
          const variantMetafields = await v1Query(
            `SELECT resource_id, namespace, key, data->>'value' AS value
             FROM "${bcTables.metafields}"
             WHERE resource_type = 'variant'
               AND namespace NOT IN ('Accessories','product_images','delivery_message')
               AND resource_id = ANY($1::bigint[])
             ORDER BY resource_id, namespace, key, id;`,
            [variantIds],
            `variant metafields ${chunkLabel}`
          );
          for (const mf of variantMetafields) (variantMetafieldsByVariant[mf.resource_id] ??= {})[`${mf.namespace}.${mf.key}`] = mf.value;
        }
        const variantsByProduct = {};
        for (const v of batchVariants) (variantsByProduct[v.product_id] ??= []).push(v);

        for (const [productIdStr, productVariants] of Object.entries(variantsByProduct)) {
          const product = productsById[productIdStr];
          if (!product) continue;
          const productImageUrls = imageUrlsByProduct[productIdStr] ?? [];
          const productMetas = productMetafieldsByProduct[productIdStr] ?? {};
          if (newProductIds.has(productIdStr)) pushLine(rowToLine(buildProductRow(product.data, productImageUrls, productMetas)));
          for (const variant of productVariants) {
            const variantMetas = variantMetafieldsByVariant[variant.id] ?? {};
            pushLine(rowToLine(buildVariantRow(product.data, variant.data, variantMetas)));
          }
        }
        for (const productId of newProductIds) {
          if (variantsByProduct[productId]) continue;
          const product = productsById[productId];
          if (!product) continue;
          const productImageUrls = imageUrlsByProduct[productId] ?? [];
          const productMetas = productMetafieldsByProduct[productId] ?? {};
          pushLine(rowToLine(buildProductRow(product.data, productImageUrls, productMetas)));
        }
        await flushPending(false);
      }

      productsProcessed += chunkIds.length;
      await fastn.db.query(`UPDATE fdx_export_jobs SET products_processed = $1, updated_at = now() WHERE id = $2`, [productsProcessed, job.id]);
    }
    await flushPending(true);

    // ---- Final delivery: FileRef passthrough -- the platform moves bytes directly from
    // object storage to SFTP; the assembled file never enters workflow memory. ----
    const finalName = `${fileName} - ${dateStr}.tsv`;
    const remotePath = `${FTP_REMOTE_PATH}/${finalName}`;

    const uploadRes = await fastn.connector.sftpServer.uploadFile({
      path: remotePath,
      file: {
        fileId: `fdx-job-${job.id}`,
        key: fileKey,
        bucket: 'fastn-files',
        size: 0,
        mime: 'text/tab-separated-values',
        name: finalName,
      },
    });
    await fastn.files.delete(localPath);

    const finalFile = { name: finalName, path: remotePath, size: uploadRes.output?.size ?? totalBytesWritten };

    await fastn.db.query(
      `UPDATE fdx_export_jobs
       SET status = 'completed', completed_at = now(), updated_at = now(),
           final_files = $2, final_folder_id = $3, products_processed = $4
       WHERE id = $1`,
      [job.id, JSON.stringify([finalFile]), FTP_REMOTE_PATH, productsProcessed]
    );

    return {
      status: 'completed',
      jobId: job.id,
      totalProducts: productIds.length,
      totalRows: totalRowsWritten,
      finalFile,
      ftpPath: FTP_REMOTE_PATH,
      hasNewColumns,
      newColumns,
      syncJobInfo,
    };
  } catch (err) {
    const message = `Job ${job.id} failed: ${err?.message || err}`;
    try {
      await fastn.db.query(
        `UPDATE fdx_export_jobs SET status = 'failed', updated_at = now(), error_message = $2 WHERE id = $1`,
        [job.id, message]
      );
    } catch (dbErr) {
      // Best-effort -- the thrown error below is still the source of truth.
    }
    throw new Error(message);
  }
}