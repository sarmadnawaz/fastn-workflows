export default async function(ctx) {
  const { input, headers } = ctx;
  // Your workflow logic here
  return { result: "updating the message again!", input };
}