export default async function (ctx) {
  const f = new Fastn({ connectors: { bigcommerce: { orgId: "managed" } } });
  const r = await f.connector.bigcommerce.listChannels({});
  const data = (r.output && r.output.data) || [];
  const options = data
    .filter((ch) => ch && ch.status !== "terminated")
    .map((ch) => ({ label: ch.id, value: ch.id }));
  return { options, cursor: "", hasNextPage: "", total: "" };
}
