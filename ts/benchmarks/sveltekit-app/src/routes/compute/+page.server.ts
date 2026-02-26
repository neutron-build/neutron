import { lcgWork } from "$lib/server/data";
import type { PageServerLoad } from "./$types";

export const load: PageServerLoad = async () => {
  const result = lcgWork(42, 140000);
  return { result };
};
