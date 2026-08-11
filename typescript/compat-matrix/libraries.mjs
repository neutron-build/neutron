// The libraries the matrix covers, and what "works" means for each.
//
// Ordered hardest-first, because the hard end is what adopters actually ask
// about. A library is listed here only with a real mount — importing a package
// proves nothing, since the interesting failures are in render: hooks that read
// React internals, `findDOMNode`, `createPortal`, context identity across the
// compat boundary, `forwardRef` shapes, `useSyncExternalStore`.
//
// `expect` runs against the SSR output. Keep it specific enough that a silently
// empty render fails: `renderToString` returning "" is the single most common
// way a compat problem hides.
import { h } from "preact";

/** @typedef {{ id: string, label: string, why: string, load: () => Promise<import("preact").VNode>, expect: (html: string) => boolean }} Case */

/** @type {Case[]} */
export const CASES = [
  {
    id: "radix-dialog",
    label: "@radix-ui/react-dialog",
    why: "Radix is the top of the adopter question list; Dialog is its controlled-state + context case.",
    load: async () => {
      const D = await import("@radix-ui/react-dialog");
      // Deliberately NOT wrapped in D.Portal. A portal has no server output —
      // that is true under React too — so portalled content renders empty and
      // says nothing about compat. The trigger and an unportalled Content are
      // the parts that actually exercise the boundary.
      return h(
        D.Root,
        { open: true },
        h(D.Trigger, null, "dialog-trigger"),
        h(D.Content, null, h(D.Title, null, "dialog-title"))
      );
    },
    expect: (html) => html.includes("dialog-trigger") && html.includes("dialog-title"),
  },
  {
    id: "radix-tabs",
    label: "@radix-ui/react-tabs",
    why: "Roving-focus context and controlled state without a portal.",
    load: async () => {
      const T = await import("@radix-ui/react-tabs");
      return h(
        T.Root,
        { defaultValue: "one" },
        h(T.List, null, h(T.Trigger, { value: "one" }, "tab-one")),
        h(T.Content, { value: "one" }, "panel-one")
      );
    },
    expect: (html) => html.includes("tab-one") && html.includes("panel-one"),
  },
  {
    id: "radix-tooltip",
    label: "@radix-ui/react-tooltip",
    why: "Provider + portal + presence, the combination that trips compat most often.",
    load: async () => {
      const T = await import("@radix-ui/react-tooltip");
      return h(
        T.Provider,
        null,
        h(T.Root, { open: true }, h(T.Trigger, null, "tip-trigger"), h(T.Portal, null, h(T.Content, null, "tip-body")))
      );
    },
    expect: (html) => html.includes("tip-trigger"),
  },
  {
    id: "radix-popover",
    label: "@radix-ui/react-popover",
    why: "Same primitive family as Dialog, different anchor machinery.",
    load: async () => {
      const P = await import("@radix-ui/react-popover");
      return h(
        P.Root,
        { open: true },
        h(P.Trigger, null, "pop-trigger"),
        h(P.Portal, null, h(P.Content, null, "pop-body"))
      );
    },
    expect: (html) => html.includes("pop-trigger"),
  },
  {
    id: "radix-dropdown-menu",
    label: "@radix-ui/react-dropdown-menu",
    why: "Menu semantics on top of the popper stack.",
    load: async () => {
      const M = await import("@radix-ui/react-dropdown-menu");
      return h(
        M.Root,
        { open: true },
        h(M.Trigger, null, "menu-trigger"),
        h(M.Portal, null, h(M.Content, null, h(M.Item, null, "menu-item")))
      );
    },
    expect: (html) => html.includes("menu-trigger"),
  },
  {
    id: "radix-select",
    label: "@radix-ui/react-select",
    why: "The heaviest Radix primitive, and the one most likely to need DOM measurement.",
    load: async () => {
      const S = await import("@radix-ui/react-select");
      return h(
        S.Root,
        { open: true, value: "a" },
        h(S.Trigger, null, h(S.Value, { placeholder: "select-placeholder" })),
        h(S.Portal, null, h(S.Content, null, h(S.Viewport, null, h(S.Item, { value: "a" }, h(S.ItemText, null, "item-a")))))
      );
    },
    expect: (html) => html.length > 0 && /select|item-a|role=/.test(html),
  },
  {
    id: "tanstack-query",
    label: "@tanstack/react-query",
    why: "useSyncExternalStore and a context-bound client — the modern-hook case.",
    load: async () => {
      const { QueryClient, QueryClientProvider, useQuery } = await import("@tanstack/react-query");
      const client = new QueryClient();
      function Probe() {
        const { status } = useQuery({ queryKey: ["k"], queryFn: async () => "v" });
        return h("span", null, `query-status:${status}`);
      }
      return h(QueryClientProvider, { client }, h(Probe, null));
    },
    expect: (html) => html.includes("query-status:"),
  },
  {
    id: "tanstack-table",
    label: "@tanstack/react-table",
    why: "Headless, hook-heavy, no DOM — should be the easy case, and proves the harness is not trivially passing.",
    load: async () => {
      const { useReactTable, getCoreRowModel, flexRender } = await import("@tanstack/react-table");
      function Probe() {
        const table = useReactTable({
          data: [{ name: "row-alpha" }],
          columns: [{ accessorKey: "name", header: "col-name" }],
          getCoreRowModel: getCoreRowModel(),
        });
        return h(
          "table",
          null,
          h(
            "tbody",
            null,
            table.getRowModel().rows.map((row) =>
              h(
                "tr",
                { key: row.id },
                row.getVisibleCells().map((cell) =>
                  h("td", { key: cell.id }, flexRender(cell.column.columnDef.cell, cell.getContext()))
                )
              )
            )
          )
        );
      }
      return h(Probe, null);
    },
    expect: (html) => html.includes("row-alpha"),
  },
  {
    id: "react-hook-form",
    label: "react-hook-form",
    why: "Refs and uncontrolled inputs; the ref shape is where compat differs.",
    load: async () => {
      const { useForm } = await import("react-hook-form");
      function Probe() {
        const { register } = useForm();
        return h("form", null, h("input", { ...register("email"), placeholder: "form-email" }));
      }
      return h(Probe, null);
    },
    expect: (html) => html.includes("form-email"),
  },
  {
    id: "framer-motion",
    label: "framer-motion",
    why: "Reads React internals more than almost anything else in common use.",
    load: async () => {
      const { motion } = await import("framer-motion");
      return h(motion.div, { initial: { opacity: 0 }, animate: { opacity: 1 } }, "motion-body");
    },
    expect: (html) => html.includes("motion-body"),
  },
  {
    id: "recharts",
    label: "recharts",
    why: "SVG charting with children introspection (Children.map over element types). Known to fail: recharts invokes its axis components outside an active render to read their configuration (`useChartWidth` -> `useContext` from `XAxisImpl`), and Preact's hooks require a current component. Render charts client-only (an island) rather than server-rendering them.",
    load: async () => {
      const { LineChart, Line, XAxis } = await import("recharts");
      return h(
        LineChart,
        { width: 320, height: 180, data: [{ x: 1, y: 2 }, { x: 2, y: 4 }] },
        h(XAxis, { dataKey: "x" }),
        h(Line, { dataKey: "y" })
      );
    },
    expect: (html) => html.includes("<svg") || html.includes("recharts"),
  },
  {
    id: "react-day-picker",
    label: "react-day-picker",
    why: "Date grids, heavy per-cell rendering.",
    load: async () => {
      const { DayPicker } = await import("react-day-picker");
      return h(DayPicker, { mode: "single", defaultMonth: new Date("2026-01-15T00:00:00Z") });
    },
    expect: (html) => html.includes("<table") || /january|2026/i.test(html),
  },
  {
    id: "react-syntax-highlighter",
    label: "react-syntax-highlighter",
    why: "Large render trees produced from a tokenizer; a common docs-site dependency.",
    load: async () => {
      const mod = await import("react-syntax-highlighter");
      const Highlighter = mod.Prism ?? mod.default;
      return h(Highlighter, { language: "js" }, "const highlighted = 1;");
    },
    expect: (html) => html.includes("highlighted"),
  },
];
