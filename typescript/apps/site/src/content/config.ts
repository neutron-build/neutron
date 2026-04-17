import { defineCollection, z } from "@neutron-build/core/content";

export const collections = {
  blog: defineCollection({
    schema: z.object({
      title: z.string(),
      description: z.string(),
      pubDate: z.coerce.date(),
      author: z.string(),
      tags: z.array(z.string()),
      draft: z.boolean().default(false),
    }),
  }),
  docs: defineCollection({
    schema: z.object({
      title: z.string(),
      description: z.string().optional(),
    }),
  }),
};
