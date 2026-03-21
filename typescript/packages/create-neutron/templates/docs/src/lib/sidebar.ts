export interface SidebarItem {
  title: string;
  slug: string;
  order: number;
  active?: boolean;
}

export interface SidebarSection {
  title: string;
  slug: string;
  order: number;
  items: SidebarItem[];
  children: SidebarSection[];
}

interface EntryInput {
  slug: string;
  data: {
    title: string;
    sidebar_label?: string;
    order?: number;
  };
}

function toTitle(segment: string): string {
  return segment
    .replace(/[-_]/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export function buildSidebarTree(
  entries: EntryInput[],
  activeSlug?: string,
): SidebarSection[] {
  const sectionMap = new Map<string, SidebarSection>();
  const rootItems: SidebarItem[] = [];

  for (const entry of entries) {
    const parts = entry.slug.split("/");

    if (parts.length === 1 || (parts.length === 2 && parts[1] === "index")) {
      rootItems.push({
        title: entry.data.sidebar_label || entry.data.title,
        slug: entry.slug,
        order: entry.data.order ?? 999,
        active: activeSlug === entry.slug,
      });
      continue;
    }

    const sectionSlug = parts[0]!;
    let section = sectionMap.get(sectionSlug);

    if (!section) {
      section = {
        title: toTitle(sectionSlug),
        slug: sectionSlug,
        order: 999,
        items: [],
        children: [],
      };
      sectionMap.set(sectionSlug, section);
    }

    const isIndex = parts[parts.length - 1] === "index";

    if (isIndex) {
      section.title = entry.data.sidebar_label || entry.data.title;
      section.order = entry.data.order ?? section.order;
    } else {
      section.items.push({
        title: entry.data.sidebar_label || entry.data.title,
        slug: entry.slug,
        order: entry.data.order ?? 999,
        active: activeSlug === entry.slug,
      });
    }
  }

  for (const section of sectionMap.values()) {
    section.items.sort((a, b) => a.order - b.order || a.title.localeCompare(b.title));
  }

  const sections = Array.from(sectionMap.values());
  sections.sort((a, b) => a.order - b.order || a.title.localeCompare(b.title));

  if (rootItems.length > 0) {
    rootItems.sort((a, b) => a.order - b.order || a.title.localeCompare(b.title));
    const rootSection: SidebarSection = {
      title: "",
      slug: "",
      order: -1,
      items: rootItems,
      children: [],
    };
    return [rootSection, ...sections];
  }

  return sections;
}
