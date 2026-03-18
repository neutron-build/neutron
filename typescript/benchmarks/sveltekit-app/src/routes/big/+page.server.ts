import { generateBigTableRows } from "$lib/server/data";
import type { PageServerLoad } from "./$types";

export const load: PageServerLoad = async () => {
  const rows = generateBigTableRows(400);
  return { rows };
};
