import { defineCollection, z } from "@neutron-build/core/content";

const tier = z.enum(["beginner", "intermediate", "master"]);

const tierAnchor = z.union([z.string(), z.literal("deferred"), z.null()]);

const reference = z.object({
  source: z.string(),
  path: z.string(),
  locator: z.string().optional(),
  pending: z.boolean().optional(),
  pointer: z.string().optional(),
});

export const collections = {
  units: defineCollection({
    schema: z.object({
      id: z.string(),
      title: z.string(),
      slug: z.string(),
      section: z.string(),
      chapter: z.string(),
      concept_catalog_id: z.string(),
      prerequisites: z.array(z.string()).default([]),
      successors: z.array(z.string()).default([]),
      tier_anchors: z.object({
        beginner: tierAnchor,
        intermediate: tierAnchor,
        master: tierAnchor,
      }),
      tiers_present: z.array(tier).min(1),
      pending_prereqs: z.boolean().default(false),
      references: z.array(reference).min(1),
      lean_module: z.string().optional(),
      lean_status: z.enum(["full", "partial", "none"]),
      lean_mathlib_gap: z.string().optional(),
      human_reviewer: z.string().optional(),
      estimated_time: z.record(z.string()).optional(),
      status: z.enum(["draft", "review", "approved", "shipped"]),
      produced_by: z.string().optional(),
      reviewed_by: z.array(z.string()).default([]),
    }),
  }),
};
