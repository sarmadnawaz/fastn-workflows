export default async function(ctx){
  const bcEnv = ctx.input?.bcEnv ?? 'devbox';
  const N = ctx.input?.shards ?? 4;
  const maxTotal = ctx.input?.maxProductsToExport ?? 100000;
  const tableNames = await fastn.envConfig.get('tableNames');
  const productsTable = `${tableNames.bigCommerce.products}_${bcEnv}`;
  const t0 = Date.now();
  const r = await fastn.db.v1.query(
    `SELECT min(id) AS lo, max(id) AS hi FROM (
       SELECT id, ntile($1) OVER (ORDER BY id) AS b
       FROM (SELECT id FROM "${productsTable}" WHERE sku IS NOT NULL AND sku <> '' ORDER BY id LIMIT $2) s
     ) t GROUP BY b ORDER BY b`, [N, maxTotal]);
  const ranges = r.rows.map((row,i)=>({ bcEnv, fileName:`DevBox DEV - shard ${i+1}of${N}`, passes:1, productPage:300, startCursor:Number(row.lo)-1, endCursor:Number(row.hi), gzipOutput:true, gzipLevel:1, shardIndex:i }));
  const results = await fastn.flow.map('export-shard', ranges, { pollMs: 4000, maxWaitMs: 3600000 });
  return { shards: results.length, elapsedMs: Date.now()-t0, allCompleted: results.every(x=>x.status==='completed'),
    totalRows: results.reduce((a,x)=>a+((x.output&&x.output.totalRows)||0),0),
    perShard: results.map(x=>({status:x.status, rows:x.output&&x.output.totalRows, file:x.output&&x.output.finalFile&&x.output.finalFile.name})) };
}
