export default async function (ctx) {
  const input = ctx.input || {};
  const headers = ctx.headers || {};
  const tenantId = headers["x-fastn-space-tenantid"] ?? null;
  const projectId = headers["x-fastn-space-id"] ?? null;
  // akeneoEnv comes from the tenant's saved Akeneo config (v1: steps.akeneoConfig.output.akeneoEnv)
  const cfg = input.configuration || input;
  const akeneoEnv = cfg.akeneoEnv ?? null;

  const tableNames = await fastn.envConfig.get("tableNames");
  const tenantsTable = tableNames && tableNames.utility ? tableNames.utility.activeTenants : null;

  // Per-tenant product-sync webhooks (v1 built _1.._7 suffixes)
  const webhookIds = [];
  for (let i = 1; i <= 7; i++) {
    webhookIds.push(i === 1
      ? `syncProductsToBC_${akeneoEnv}_${tenantId}`
      : `syncProductsToBC_${akeneoEnv}_${tenantId}_${i}`);
  }
  // Shared akeneo webhooks (only removed when this is the LAST tenant on the env)
  const akeneoWebhooks = [];
  for (let i = 1; i <= 7; i++) {
    akeneoWebhooks.push(i === 1
      ? `cacheAkeneoEntities_${akeneoEnv}`
      : `cacheAkeneoEntities_${akeneoEnv}_${i}`);
  }
  akeneoWebhooks.push(`fetchAkProductsMonitor_${akeneoEnv}`);

  // Is this the last tenant on this akeneoEnv?
  let tenantCount = null;
  if (tenantsTable && akeneoEnv != null) {
    const res = await fastn.db.v1.query(
      `SELECT COUNT(*)::int AS count FROM ${tenantsTable} WHERE akeneo_env = $1`,
      [akeneoEnv]
    );
    tenantCount = res.rows && res.rows[0] ? res.rows[0].count : null;
  }
  const toDelete = tenantCount === 1 ? webhookIds.concat(akeneoWebhooks) : webhookIds.slice();

  // v1 deleted each id via the fastn-platform community 'deleteWebhook' connector
  // (body { withResources: true, id }). No v2 equivalent connector exists yet, so this
  // teardown is a STUB — surface which subscriptions must be removed.
  console.warn("deactivate: v2 webhook/scheduler teardown not wired — pending connector-builder. Would delete:", JSON.stringify(toDelete));

  return {
    message: "deactivated",
    _migrationNotes: {
      pendingWebhookTeardown: toDelete,
      lastTenantOnEnv: tenantCount === 1,
      tenantCount,
      reason: "v1 community 'deleteWebhook' connector has no v2 equivalent; teardown stubbed."
    }
  };
}
