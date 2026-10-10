export default async function (ctx) {
  const input = (ctx && ctx.input) || {};
  const ms = Math.min(Math.max(Number(input.ms) || 5000, 0), 15000);
  const startedAt = new Date().toISOString();
  await new Promise((resolve) => setTimeout(resolve, ms));
  return { ok: true, sleptMs: ms, startedAt, finishedAt: new Date().toISOString(), tag: input.tag || null };
}
