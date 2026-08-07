export default async function(ctx) {
  const lineCount = (ctx.input && ctx.input.lineCount) || 10;
  const fileName = (ctx.input && ctx.input.fileName) || "v2 file test.txt";
  const parentId = (ctx.input && ctx.input.parentId) || "root";
  const storagePath = (ctx.input && ctx.input.storagePath) || "v2-file-test/v2 file test.txt";

  // Start clean so re-runs don't keep appending onto stale content from a prior run
  if (await fastn.files.exists(storagePath)) {
    await fastn.files.delete(storagePath);
  }

  // Append one chunk at a time -- no growing in-memory string. In a real
  // scenario this loop is a DB pagination loop: read a small page of rows,
  // append that page's text, discard it, fetch the next page. Memory stays
  // flat whether the source table has 10 rows or 10 million.
  for (let i = 1; i <= lineCount; i++) {
    const line = `Line ${i} in file\n`;
    await fastn.files.append(storagePath, line);
  }

  // The Drive upload action's contract takes the full body as one string --
  // there's no chunked/streaming upload action exposed -- so this single
  // read is the one point the finished file has to be materialized in memory.
  const finalContent = await fastn.files.read(storagePath);

  const upload = await fastn.connector.googleDrive.uploadFile({
    name: fileName,
    content: finalContent,
    mimeType: "text/plain",
    parentId: parentId
  });

  if (!upload.success) {
    throw new Error(`Upload failed: status=${upload.status} error=${JSON.stringify(upload.error)}`);
  }

  return {
    linesWritten: lineCount,
    storagePath,
    fileId: upload.output.id,
    fileName: upload.output.name,
    size: upload.output.size
  };
}