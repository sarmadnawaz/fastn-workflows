export default async function (ctx) {
  const headers = ctx.headers || {};
  const tenantId = headers["x-fastn-space-tenantid"] ?? null;
  const tableNames = await fastn.envConfig.get("tableNames");
  const tenantsTable = tableNames && tableNames.utility ? tableNames.utility.activeTenants : null;

  if (tenantsTable && tenantId) {
    await fastn.db.v1.query(`DELETE FROM ${tenantsTable} WHERE tenant_id = $1`, [tenantId]);
  }
  return { message: "deactivated" };
}
