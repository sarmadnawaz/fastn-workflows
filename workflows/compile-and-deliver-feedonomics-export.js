export default async function (ctx) {
  const input = ctx.input || {};
  const { jobId } = input;
  if (!jobId) return { status: 'error', message: 'jobId is required' };

  const app = new Fastn({ connectors: { googleDrive: { orgId: 'managed' }, googleSheets: { orgId: 'managed' } } });

  const jobRows = await fastn.db.query(
    `SELECT id, base_file_name, bc_env, created_at, gdrive_folder_id FROM fdx_export_jobs WHERE id = $1`,
    [jobId]
  );
  if (jobRows.length === 0) return { status: 'error', message: `job ${jobId} not found` };
  const job = jobRows[0];

  // Preserves the fix agreed for v1's bug: order strictly by batch_number, never by filename.
  const batches = await fastn.db.query(
    `SELECT spreadsheet_id, batch_number FROM fdx_export_job_batches WHERE job_id = $1 AND status = 'completed' ORDER BY batch_number`,
    [jobId]
  );
  if (batches.length === 0) return { status: 'error', message: `no completed batches for job ${jobId}` };

  const folderIds = (await fastn.envConfig.get('feedonomicsDriveFolderIds')) || {};
  const dateStr = String(job.created_at).split('T')[0];

  // ---- Compile: read each batch sheet's values directly, drop header from every batch after the first ----
  let combinedRows = [];
  for (let i = 0; i < batches.length; i++) {
    const b = batches[i];
    const res = await app.connector.googleSheets.getValues({ spreadsheetId: b.spreadsheet_id, range: 'Sheet1' });
    const values = res.output.values || [];
    combinedRows = combinedRows.concat(i === 0 ? values : values.slice(1));
  }

  // ---- Serialize to real TSV text and write it as an actual Drive file via uploadFile. ----
  // No standard TSV escaping mechanism exists (unlike CSV's quoting), so embedded tabs/newlines
  // within a cell are flattened to spaces to keep columns aligned.
  function tsvCell(v) {
    if (v === null || v === undefined) return '';
    return String(v).replace(/[\t\r\n]+/g, ' ');
  }
  function tsvText(rowsSlice) {
    return rowsSlice.map((r) => r.map(tsvCell).join('\t')).join('\n');
  }

  const finalTitle = `${job.base_file_name} - ${dateStr}`;
  const parentFolderId = folderIds[job.bc_env] || folderIds.default || null; // fixed: was folderIds.get(env) in v1, a bug on a plain object
  const uploadParentId = parentFolderId || 'root';

  // uploadFile enforces a hard 10,485,760-byte (10MB) request-body cap. Try the whole export as one
  // file first (the common case); only if that fails from size, split in half and retry each half,
  // recursing until every piece clears the cap. uploadFile only creates a file on success, so a failed
  // attempt never leaves anything to clean up.
  const finalFiles = [];
  let partCounter = 0;
  async function uploadChunk(rowsSlice, name) {
    const content = tsvText(rowsSlice);
    const res = await app.connector.googleDrive.uploadFile({
      name,
      content,
      mimeType: 'text/tab-separated-values',
      parentId: uploadParentId,
    });
    finalFiles.push(res.output);
  }
  async function uploadResilient(rowsSlice, isWholeJob) {
    const name = isWholeJob ? `${finalTitle}.tsv` : `${finalTitle} - part${partCounter + 1}.tsv`;
    try {
      await uploadChunk(rowsSlice, name);
      if (!isWholeJob) partCounter += 1;
    } catch (e) {
      if (rowsSlice.length <= 1) throw e;
      const mid = Math.ceil(rowsSlice.length / 2);
      await uploadResilient(rowsSlice.slice(0, mid), false);
      await uploadResilient(rowsSlice.slice(mid), false);
    }
  }
  await uploadResilient(combinedRows, true);

  // ---- Cleanup: delete the raw per-batch-sheets folder (v1 kept this for testing; now always deleted) ----
  if (job.gdrive_folder_id) {
    await app.connector.googleDrive.deleteFile({ fileId: job.gdrive_folder_id });
  }

  const fileUrl = (id) => `https://drive.google.com/file/d/${id}/view`;
  const finalFilesOut = finalFiles.map((f) => ({ id: f.id, name: f.name, size: f.size, url: fileUrl(f.id) }));

  // Persist the FINAL delivery location on the job row -- gdrive_folder_id only ever pointed at the
  // temporary raw-batch folder we just deleted, so without this a completed job had no queryable
  // link to where the deliverable actually landed.
  await fastn.db.query(
    `UPDATE fdx_export_jobs
     SET status = 'completed', completed_at = now(), updated_at = now(),
         gdrive_folder_id = NULL, final_files = $2, final_folder_id = $3,
         final_spreadsheet_id = $4, final_spreadsheet_url = $5
     WHERE id = $1`,
    [jobId, JSON.stringify(finalFilesOut), parentFolderId, finalFilesOut[0]?.id ?? null, finalFilesOut[0]?.url ?? null]
  );

  return {
    status: 'completed',
    jobId,
    finalFiles: finalFilesOut,
    finalFolderId: parentFolderId,
    batchesCompiled: batches.length,
    totalRows: combinedRows.length,
  };
}