export default async function (ctx) {
  const input = ctx.input || {};
  const headers = ctx.headers || {};
  const tenantId = headers["x-fastn-space-tenantid"] ?? "";
  // akeneoEnv comes from the tenant's saved widget configuration (v1: steps.akeneoConfig.output.akeneoEnv).
  const cfg = input.configuration || input;
  const akeneoEnv = cfg.akeneoEnv ?? null;

  const tableNames = await fastn.envConfig.get("tableNames");
  const tenantsTable = tableNames && tableNames.utility ? tableNames.utility.activeTenants : null;

  if (tenantsTable) {
    await fastn.db.v1.query(
      `INSERT INTO ${tenantsTable} (tenant_id, akeneo_env) VALUES ($1, $2) ` +
      `ON CONFLICT (tenant_id) DO UPDATE SET akeneo_env = EXCLUDED.akeneo_env`,
      [tenantId, akeneoEnv]
    );
  }
  return { userMessage: "Configurations updated." };
}
