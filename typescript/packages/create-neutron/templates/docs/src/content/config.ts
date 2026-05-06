import { defineCollection, z } from "@neutron-build/core/content";

export const collections = {
  docs: defineCollection({
    schema: z.object({
      title: z.string(),
      description: z.string().optional(),
      sidebar_label: z.string().optional(),
      order: z.number().optional(),
      draft: z.boolean().default(false),
    }),
  }),
};
