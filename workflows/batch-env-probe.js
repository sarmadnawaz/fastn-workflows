export default async function (ctx) {
  // Batch-runner environment probe. The Cloud Run job runs the same user code as
  // the workers but receives none of the cluster's configmap, so a missing env
  // var degrades silently to a wrong code default rather than failing loudly.
  // This exercises the two capabilities that break that way.
  const out = { ranAt: ctx.executionId || null };

  // fastn.db -> needs WORKSPACE_DB_URL (pods read it from fastn-db-credentials,
  // which Cloud Run cannot reach). Unset falls through to POSTGRES_* and fails.
  try {
    const r = await fastn.db.query('select current_database() as db');
    const rows = (r && r.rows) || r || [];
    out.db = { ok: true, database: rows[0] ? rows[0].db : null };
  } catch (e) {
    out.db = { ok: false, error: String((e && e.message) || e) };
  }

  // fastn.files -> needs GCS_FILES_BUCKET. Unset defaults to the literal
  // "fastn-files", a bucket that does not exist.
  const path = 'batch-env-probe/' + (ctx.executionId || 'run') + '.txt';
  try {
    await fastn.files.write(path, 'probe');
    const back = await fastn.files.read(path);
    out.files = { ok: back === 'probe', roundTrip: back === 'probe' };
    await fastn.files.delete(path);
  } catch (e) {
    out.files = { ok: false, error: String((e && e.message) || e) };
  }

  out.allOk = !!(out.db && out.db.ok && out.files && out.files.ok);
  return out;
}
