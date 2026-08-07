export default async function(ctx) {
  const { input, headers } = ctx;
  // Your workflow logic here
  const { rows } = await fastn.db.v1.query(
    `SELECT *
     FROM akeneo_categories_prod
     LIMIT 1;`
  );
  return { rows };
}