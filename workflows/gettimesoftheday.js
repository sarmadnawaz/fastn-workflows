export default async function (ctx) {
  const interval = (ctx.input && Number(ctx.input.interval)) > 0 ? Number(ctx.input.interval) : 30;
  const options = [];
  for (let mins = 0; mins < 24 * 60; mins += interval) {
    const h = String(Math.floor(mins / 60)).padStart(2, "0");
    const m = String(mins % 60).padStart(2, "0");
    const t = `${h}:${m}`;
    options.push({ label: t, value: t });
  }
  return { options };
}
