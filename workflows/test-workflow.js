export default async function(ctx) {
  const { input, headers } = ctx;
  // Your workflow logic here
  return { result: "Updated the message!", input };
}