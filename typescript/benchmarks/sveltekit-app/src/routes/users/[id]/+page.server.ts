import { error } from "@sveltejs/kit";
import { USERS } from "$lib/server/data";
import type { PageServerLoad } from "./$types";

export const load: PageServerLoad = async ({ params }) => {
  const id = Number(params.id);
  const name = USERS[id];

  if (!name) {
    throw error(404, `User ${params.id} not found`);
  }

  return { id, name };
};
