export default async function (ctx) {
  const c = fastn.connector.sftpServer;
  const out = { deleted: [] };
  const t0 = Date.now();
  const gzKey = await fastn.files.gzip('feedonomics-exports/job-83.tsv', undefined, { level: 1 });
  out.gzKey = gzKey; out.gzipMs = Date.now() - t0;
  let files = [];
  try { files = (await c.listFiles({ path: '/incoming/test' }))?.output ?? []; }
  catch (e) { out.listErr = String(e?.message || e); }
  for (const f of files) {
    if (f.type && f.type !== 'file') continue;
    try { await c.deleteFile({ path: `/incoming/test/${f.name}` }); out.deleted.push(f.name); }
    catch (e) { out.deleted.push(f.name + ' [skip]'); }
  }
  try {
    const res = await c.uploadFile({
      path: '/incoming/test/DevBox DEV - 2 Million (9-pass).tsv.gz',
      file: { fileId: 'deliver-83-gz', key: gzKey, bucket: 'fastn-files', size: 0, mime: 'application/gzip', name: 'DevBox DEV - 2 Million (9-pass).tsv.gz' },
    });
    out.status = 'DELIVERED_GZ'; out.deliver = res.output ?? res; out.totalMs = Date.now() - t0;
  } catch (e) { out.status = 'deliver_failed'; out.deliverError = String(e?.message || e); }
  return out;
}
