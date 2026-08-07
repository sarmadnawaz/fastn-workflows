export default async function (ctx) {
  const input = ctx.input || {};
  const { jobId, batchId, baseFileName, bcEnv } = input;
  if (!jobId || !batchId || !bcEnv) {
    return { status: 'error', message: 'jobId, batchId and bcEnv are required' };
  }

  const app = new Fastn({ connectors: { googleDrive: { orgId: 'managed' }, googleSheets: { orgId: 'managed' } } });

  // googleSheets.appendValues fails once a single call's payload gets too large; the exact
  // threshold varies with cell content (observed failures between ~20-50 rows at 448 columns).
  // Rather than guess a fixed-safe chunk size, halve and retry on failure until it succeeds.
  async function appendRowsResilient(spreadsheetId, range, rows) {
    if (rows.length === 0) return 0;
    try {
      await app.connector.googleSheets.appendValues({ spreadsheetId, range, valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', values: rows });
      return rows.length;
    } catch (e) {
      if (rows.length === 1) throw e;
      const mid = Math.ceil(rows.length / 2);
      const a = await appendRowsResilient(spreadsheetId, range, rows.slice(0, mid));
      const b = await appendRowsResilient(spreadsheetId, range, rows.slice(mid));
      return a + b;
    }
  }

  const tableNames = await fastn.envConfig.get('tableNames');
  const bcTables = {
    products: `${tableNames.bigCommerce.products}_${bcEnv}`,
    variants: `${tableNames.bigCommerce.productVariants}_${bcEnv}`,
    metafields: `${tableNames.bigCommerce.metafields}_${bcEnv}`,
    images: `${tableNames.bigCommerce.productImages}_${bcEnv}`,
  };

  // ---- Load job + activate batch (v2-native tracking tables) ----
  const jobRows = await fastn.db.query(
    `SELECT id, total_batches, file_header, created_at, gdrive_folder_id FROM fdx_export_jobs WHERE id = $1`,
    [jobId]
  );
  if (jobRows.length === 0) return { status: 'error', message: `job ${jobId} not found` };
  const job = jobRows[0];
  const headers = job.file_header; // unified: same header used for the row AND for data column ordering

  const batchRows = await fastn.db.query(
    `UPDATE fdx_export_job_batches SET status = 'active', updated_at = now() WHERE id = $1 RETURNING id, batch_number, product_ids`,
    [batchId]
  );
  if (batchRows.length === 0) return { status: 'error', message: `batch ${batchId} not found` };
  const batch = batchRows[0];
  const productIds = batch.product_ids; // already a native JS array (jsonb)

  const dateStr = String(job.created_at).split('T')[0];
  const sheetFileName = `${baseFileName} - ${bcEnv} - ${dateStr} (${batch.batch_number} of ${job.total_batches})`;

  // ---- Create + place the per-batch spreadsheet ----
  const sheetRes = await app.connector.googleSheets.createSpreadsheet({ title: sheetFileName });
  const spreadsheetId = sheetRes.output.spreadsheetId;
  const spreadsheetUrl = sheetRes.output.spreadsheetUrl;

  await app.connector.googleDrive.moveFile({
    fileId: spreadsheetId,
    newParentId: job.gdrive_folder_id,
    oldParentId: 'root',
  });

  await appendRowsResilient(spreadsheetId, 'Sheet1', [headers]);

  // ---- Build rows for every product in this batch, 200 products at a time ----
  const metafieldColumns = new Set(
    headers.filter((h) => !h.startsWith('product_') && !h.startsWith('variant_') && !h.startsWith('image_') && h !== 'resource_type')
  );
  function serializeValue(v) {
    if (v === null || v === undefined) return '';
    if (typeof v === 'object') return JSON.stringify(v).slice(0, 50000);
    return String(v).slice(0, 50000);
  }
  function buildProductRow(productJson, productImages, productMetas) {
    const row = {};
    for (const header of headers) {
      if (header === 'resource_type') row[header] = 'product';
      else if (header.startsWith('product_')) row[header] = serializeValue(productJson[header.slice(8)]);
      else if (header.startsWith('image_')) {
        const img = productImages[parseInt(header.slice(6), 10) - 1];
        row[header] = img ? img.data.url_tiny : '';
      } else if (header.startsWith('variant_')) row[header] = '';
      else if (metafieldColumns.has(header)) row[header] = serializeValue(productMetas[header] ?? '');
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
      else if (metafieldColumns.has(header)) row[header] = serializeValue(variantMetas[header] ?? '');
    }
    return row;
  }

  const PRODUCT_CHUNK = 200;
  const VARIANT_CHUNK = 200;
  const APPEND_TARGET = 200; // write to the sheet in modest bursts; appendRowsResilient handles any failures
  let pendingRows = [];
  let totalRowsWritten = 0;

  async function flushPending(force) {
    while (pendingRows.length >= APPEND_TARGET || (force && pendingRows.length > 0)) {
      const chunk = pendingRows.splice(0, APPEND_TARGET);
      totalRowsWritten += await appendRowsResilient(spreadsheetId, 'Sheet1', chunk.map((row) => headers.map((h) => row[h] ?? '')));
      if (!force) break;
    }
  }

  for (let i = 0; i < productIds.length; i += PRODUCT_CHUNK) {
    const chunkIds = productIds.slice(i, i + PRODUCT_CHUNK);

    const products = (await fastn.db.v1.query(`SELECT * FROM "${bcTables.products}" WHERE id = ANY($1::bigint[]) ORDER BY id;`, [chunkIds])).rows;
    const images = (await fastn.db.v1.query(`SELECT id, product_id, is_thumbnail, data FROM "${bcTables.images}" WHERE product_id = ANY($1::bigint[]) ORDER BY product_id, id;`, [chunkIds])).rows;
    const productMetafields = (await fastn.db.v1.query(`SELECT id, resource_id, namespace, key, data FROM "${bcTables.metafields}" WHERE resource_type = 'product' AND resource_id = ANY($1::bigint[]) ORDER BY resource_id, namespace, key, id;`, [chunkIds])).rows;
    const variants = (await fastn.db.v1.query(`SELECT id, product_id, sku, data FROM "${bcTables.variants}" WHERE product_id = ANY($1::bigint[]) ORDER BY product_id, id;`, [chunkIds])).rows;

    const productsById = {};
    for (const p of products) productsById[p.id] = p;
    const imagesByProduct = {};
    for (const img of images) (imagesByProduct[img.product_id] ??= []).push(img);
    const productMetafieldsByProduct = {};
    for (const mf of productMetafields) (productMetafieldsByProduct[mf.resource_id] ??= {})[`${mf.namespace}.${mf.key}`] = mf.data.value;

    // Split this chunk's variants into sub-batches of 200, tracking first-occurrence product ids (as strings -- v1 bridge returns bigint as strings)
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
        const variantMetafields = (await fastn.db.v1.query(
          `SELECT id, resource_id, namespace, key, data FROM "${bcTables.metafields}" WHERE resource_type = 'variant' AND namespace NOT IN ('Accessories','product_images') AND resource_id = ANY($1::bigint[]) ORDER BY resource_id, namespace, key, id;`,
          [variantIds]
        )).rows;
        for (const mf of variantMetafields) (variantMetafieldsByVariant[mf.resource_id] ??= {})[`${mf.namespace}.${mf.key}`] = mf.data.value;
      }
      const variantsByProduct = {};
      for (const v of batchVariants) (variantsByProduct[v.product_id] ??= []).push(v);

      for (const [productIdStr, productVariants] of Object.entries(variantsByProduct)) {
        const product = productsById[productIdStr];
        if (!product) continue;
        const productImages = (imagesByProduct[productIdStr] ?? []).slice(0, 5);
        const productMetas = productMetafieldsByProduct[productIdStr] ?? {};
        if (newProductIds.has(productIdStr)) pendingRows.push(buildProductRow(product.data, productImages, productMetas));
        for (const variant of productVariants) {
          const variantMetas = variantMetafieldsByVariant[variant.id] ?? {};
          pendingRows.push(buildVariantRow(product.data, variant.data, variantMetas));
        }
      }
      for (const productId of newProductIds) {
        if (variantsByProduct[productId]) continue;
        const product = productsById[productId];
        if (!product) continue;
        const productImages = (imagesByProduct[productId] ?? []).slice(0, 5);
        const productMetas = productMetafieldsByProduct[productId] ?? {};
        pendingRows.push(buildProductRow(product.data, productImages, productMetas));
      }
      await flushPending(false);
    }
  }
  await flushPending(true);

  // ---- Mark batch complete, advance job ----
  await fastn.db.query(
    `UPDATE fdx_export_job_batches SET status = 'completed', spreadsheet_id = $1, spreadsheet_url = $2, updated_at = now() WHERE id = $3`,
    [spreadsheetId, spreadsheetUrl, batchId]
  );
  await fastn.db.query(`UPDATE fdx_export_jobs SET batches_completed = batches_completed + 1, updated_at = now() WHERE id = $1`, [jobId]);

  const nextBatchRows = await fastn.db.query(
    `SELECT id FROM fdx_export_job_batches WHERE job_id = $1 AND status = 'pending' ORDER BY id LIMIT 1`,
    [jobId]
  );

  if (nextBatchRows.length > 0) {
    await fastn.flow.invokeAsync('export-products-batch-to-gsheets', {
      jobId,
      batchId: nextBatchRows[0].id,
      baseFileName,
      bcEnv,
    });
    return { status: 'batch_completed', batchId, rowsWritten: totalRowsWritten, nextBatchId: nextBatchRows[0].id };
  }

  await fastn.db.query(`UPDATE fdx_export_jobs SET status = 'batches_complete', updated_at = now() WHERE id = $1`, [jobId]);
  await fastn.flow.invokeAsync('compile-and-deliver-feedonomics-export', { jobId });
  return { status: 'all_batches_completed', batchId, rowsWritten: totalRowsWritten };
}