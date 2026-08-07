export default async function (ctx) {
  const input = ctx.input || {};
  const bcEnv = input.bcEnv;
  if (!bcEnv) {
    return { status: 'error', message: 'bcEnv is required' };
  }
  const fileName = input.fileName || 'Belami Data';
  const startOffset = input.startOffset || 0;
  const maxProductsToExport = input.maxProductsToExport ?? null;
  const syncJobId = input.syncJobId ?? null;

  const app = new Fastn({ connectors: { googleDrive: { orgId: 'managed' }, googleSheets: { orgId: 'managed' } } });

  const tableNames = await fastn.envConfig.get('tableNames');
  const baselineHeader = (await fastn.envConfig.get('feedonomicsFileHeader')) || [];
  const folderIds = (await fastn.envConfig.get('feedonomicsDriveFolderIds')) || {};

  const bcTables = {
    products: `${tableNames.bigCommerce.products}_${bcEnv}`,
    variants: `${tableNames.bigCommerce.productVariants}_${bcEnv}`,
    metafields: `${tableNames.bigCommerce.metafields}_${bcEnv}`,
  };
  const syncJobsTable = `${tableNames.utility.syncJobs}_${bcEnv}`;
  const jobBatchesTable = `${tableNames.utility.jobBatches}_${bcEnv}`;

  // ---- Full vs incremental ----
  let cutoffDate = null;
  let syncJobInfo = null;
  if (syncJobId) {
    const syncJobRows = (await fastn.db.v1.query(`SELECT * FROM "${syncJobsTable}" WHERE id = $1`, [syncJobId])).rows;
    if (syncJobRows.length === 0) {
      return { status: 'error', message: `syncJobId ${syncJobId} not found in ${syncJobsTable}` };
    }
    const countRows = (await fastn.db.v1.query(
      `SELECT COALESCE(SUM(products_synced), 0) AS total FROM "${jobBatchesTable}" WHERE job_id = $1`,
      [syncJobId]
    )).rows;
    const totalSynced = Number(countRows[0].total);
    if (totalSynced === 0) {
      return { status: 'not_initiated', reason: 'No products synced for the given syncJobId' };
    }
    cutoffDate = syncJobRows[0].created_on;
    syncJobInfo = { id: syncJobRows[0].id, created_on: syncJobRows[0].created_on, totalSynced };
  }

  // ---- Product selection (id + variant_count only, for batching) ----
  let productQuery = `
    SELECT p.id AS product_id, COUNT(v.id) AS variant_count
    FROM "${bcTables.products}" p
    LEFT JOIN "${bcTables.variants}" v ON v.product_id = p.id
    WHERE p.sku IS NOT NULL AND p.sku <> ''`;
  const productParams = [];
  if (cutoffDate) {
    productParams.push(cutoffDate);
    productQuery += ` AND p.synced_at >= $${productParams.length}`;
  }
  productQuery += ` GROUP BY p.id ORDER BY p.id`;
  if (maxProductsToExport) {
    productParams.push(maxProductsToExport);
    productQuery += ` LIMIT $${productParams.length}`;
  }
  productParams.push(startOffset);
  productQuery += ` OFFSET $${productParams.length}`;

  const productRows = (await fastn.db.v1.query(productQuery, productParams)).rows;
  if (productRows.length === 0) {
    return { status: 'not_initiated', reason: 'No products matched the export criteria' };
  }

  // ---- Header discovery ----
  // NOTE: when cutoffDate is set (incremental), this scan is filtered by synced_at for performance.
  // Full-export mode (no syncJobId) scans the whole metafields table and is expected to be slower.
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

  const metafieldKeyRows = (await fastn.db.v1.query(mfQuery, mfParams)).rows;
  const sampleProduct = (await fastn.db.v1.query(`SELECT data FROM "${bcTables.products}" LIMIT 1`, [])).rows[0];
  const sampleVariant = (await fastn.db.v1.query(`SELECT data FROM "${bcTables.variants}" LIMIT 1`, [])).rows[0];

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
    // Persist the merged header back to config so future runs' baseline reflects the new columns.
    await fastn.envConfig.set('feedonomicsFileHeader', headers);
  }

  // ---- Batch by Google Sheets 10M-cell limit ----
  const CELL_LIMIT = 10000000;
  const columnCount = headers.length;
  const EFFECTIVE_LIMIT = CELL_LIMIT - columnCount * 500 - columnCount;
  const batches = [];
  let currentBatch = [];
  let currentRowCount = 0;
  for (const p of productRows) {
    const variantCount = parseInt(p.variant_count) || 0;
    const rowsForProduct = 1 + variantCount;
    if (currentBatch.length > 0 && (currentRowCount + rowsForProduct) * columnCount > EFFECTIVE_LIMIT) {
      batches.push(currentBatch);
      currentBatch = [];
      currentRowCount = 0;
    }
    currentBatch.push(p.product_id);
    currentRowCount += rowsForProduct;
  }
  if (currentBatch.length > 0) batches.push(currentBatch);

  // ---- Create job row ----
  const jobRows = await fastn.db.query(
    `INSERT INTO fdx_export_jobs (bc_env, sync_job_id, max_products, total_batches, status, base_file_name, file_header, has_new_columns, new_columns)
     VALUES ($1,$2,$3,$4,'active',$5,$6,$7,$8) RETURNING id, created_at`,
    [bcEnv, syncJobId, maxProductsToExport, batches.length, fileName, JSON.stringify(headers), hasNewColumns, JSON.stringify(newColumns)]
  );
  const job = jobRows[0];
  const dateStr = String(job.created_at).split('T')[0];

  // ---- Drive folder for this job's raw per-batch sheets ----
  // Fall back to a shared "Others" holding folder when bcEnv has no mapped Drive folder yet,
  // instead of failing or silently moving into an undefined parent.
  const parentFolderId = folderIds[bcEnv] || folderIds.default || null;
  const folderResolution = folderIds[bcEnv] ? 'mapped' : (folderIds.default ? 'default' : 'none');
  const folderRes = await app.connector.googleDrive.createFolder({ name: `Exported Data ${dateStr}` });
  if (parentFolderId) {
    await app.connector.googleDrive.moveFile({
      fileId: folderRes.output.id,
      newParentId: parentFolderId,
      oldParentId: 'root',
    });
  }
  await fastn.db.query(`UPDATE fdx_export_jobs SET gdrive_folder_id = $1, updated_at = now() WHERE id = $2`, [
    folderRes.output.id,
    job.id,
  ]);

  // ---- Create batch rows ----
  let firstBatchId = null;
  for (let i = 0; i < batches.length; i++) {
    const productIds = batches[i];
    const inserted = await fastn.db.query(
      `INSERT INTO fdx_export_job_batches (job_id, batch_number, product_ids, product_count, status)
       VALUES ($1,$2,$3,$4,'pending') RETURNING id`,
      [job.id, i + 1, JSON.stringify(productIds), productIds.length]
    );
    if (i === 0) firstBatchId = inserted[0].id;
  }

  // ---- Kick off batch 1 ----
  await fastn.flow.invokeAsync('export-products-batch-to-gsheets', {
    jobId: job.id,
    batchId: firstBatchId,
    baseFileName: fileName,
    bcEnv,
  });

  return {
    status: 'initiated',
    jobId: job.id,
    gdriveFolderId: folderRes.output.id,
    folderResolution,
    totalProducts: productRows.length,
    totalBatches: batches.length,
    hasNewColumns,
    newColumns,
    syncJobInfo,
  };
}