export default async function(ctx) {
  const { input, headers } = ctx;
  // Your workflow logic here
  return { result: "Updated this message 1234", input };
}