export default async function (ctx) {
  const f = new Fastn({ connectors: { akeneopim: { orgId: "managed" } } });
  const r = await f.connector.akeneopim.listChannels({ limit: 100 });
  const items = (r.output && r.output._embedded && r.output._embedded.items) || [];
  const options = items
    .filter((it) => it && it.code != null)
    .map((it) => ({ label: it.code, value: it.code }));
  return { options, cursor: "", hasNextPage: "", total: "" };
}
