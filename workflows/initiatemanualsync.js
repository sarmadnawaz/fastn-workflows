export default async function (ctx) {
  const input = ctx.input || {};
  const headers = ctx.headers || {};
  const tenantId = headers["x-fastn-space-tenantid"] ?? "";
  const bcEnv = tenantId; // v1 mapped bcEnv from the x-fastn-space-tenantid header
  const cfg = input.configuration || input;
  const akeneoEnv = cfg.akeneoEnv ?? null;

  const tableNames = await fastn.envConfig.get("tableNames");
  const util = (tableNames && tableNames.utility) || {};
  const productFetchJobsTable = `${util.productFetchJobs}_${akeneoEnv}`;
  const syncJobsTable = `${util.syncJobs}_${bcEnv}`;

  // Guard: reject if a fetch job is already active
  const fetchJob = await fastn.db.v1.query(
    `SELECT * FROM "${productFetchJobsTable}" WHERE status = 'active' ORDER BY created_on DESC LIMIT 1`, []);
  if (fetchJob.rows && fetchJob.rows.length > 0 && fetchJob.rows[0].id != null) {
    return { userMessage: "There is another fetch job in progress. Please try again later." };
  }
  // Guard: reject if a sync job is already active
  const syncJob = await fastn.db.v1.query(
    `SELECT * FROM "${syncJobsTable}" WHERE status = 'active' ORDER BY created_on DESC LIMIT 1`, []);
  if (syncJob.rows && syncJob.rows.length > 0 && syncJob.rows[0].id != null) {
    return { userMessage: "There is another sync in progress. You can try again once the active sync is complete." };
  }

  // v1 triggered the custom fetchAkeneoProductsToDB_v1 connector (wraps the
  // fetchAkeneoProductsToDB flow — NOT a widget flow, not yet migrated).
  const fetchPayload = { initiateSync: true, akeneoEnv, source: "widget", fullSync: false, initiateSyncForSingleTenant: true };
  let triggered = false, triggerNote = null;
  try {
    await fastn.flow.invokeAsync("fetchAkeneoProductsToDB", fetchPayload, { headers: { "x-fastn-space-tenantid": tenantId } });
    triggered = true;
  } catch (e) {
    triggerNote = "fetchAkeneoProductsToDB flow not present in v2 yet (out of widget-flow scope) — migrate it for the sync trigger to work. " + String((e && e.message) || e);
    console.warn("initiateManualSync: " + triggerNote);
  }

  return Object.assign(
    { userMessage: "Sync has been initiated." },
    triggered ? {} : { _migrationNotes: { pendingUpstreamFlow: "fetchAkeneoProductsToDB", reason: triggerNote } }
  );
}
