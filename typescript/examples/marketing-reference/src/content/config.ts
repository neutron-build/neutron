import { defineCollection, z } from "neutron/content";

export const collections = {
  blog: defineCollection({
    schema: z.object({
      title: z.string(),
      draft: z.boolean().default(false),
    }),
  }),
};
