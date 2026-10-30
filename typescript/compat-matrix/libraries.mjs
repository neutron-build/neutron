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
    why: "SVG charting with children introspection (Children.map over element types). Server-renders completely — wrapper, svg, axis, data points. This row was recorded as a hard failure from the matrix's first publication (`Cannot read properties of undefined (reading 'context')`) and diagnosed as recharts invoking axis components outside an active render; that was the harness's own artifact, not the library's: recharts ships a CJS lib build, and its `require(\"react\")` loaded the shim's CJS entry — a second, CJS preact instance whose `useContext` never sees a component. With one preact instance (compat-matrix/node-hooks.mjs) the same version renders fine.",
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

  // The Fylun gate: the live client-rendering dependencies of a real app
  // (Fylun) being evaluated for a port, pinned at its exact locked versions.
  // These rows decide whether that port proceeds, so each one mounts what the
  // app actually mounts and asserts what a correct server render of it
  // produces — including the two whose correct server render is less than the
  // full client experience (tiptap's ProseMirror view and monaco's editor are
  // client-mounted by design; the rows say so). Server-only and dead
  // dependencies are deliberately absent: the "ai" SDK never renders, and the
  // rest have zero imports in the app.
  {
    id: "fylun-tiptap",
    label: "@tiptap/react",
    why: "Fylun's editor (@tiptap/react + @tiptap/pm + @tiptap/starter-kit, all 3.30.1). ProseMirror owns its DOM outside React, so a server render can only ever produce the mount point: with `immediatelyRender: false` (Tiptap's documented SSR mode — it logs its own SSR-detected notice) EditorContent renders exactly one wrapper div, and the document itself is created client-side.",
    load: async () => {
      const { useEditor, EditorContent } = await import("@tiptap/react");
      const StarterKit = (await import("@tiptap/starter-kit")).default;
      function Probe() {
        const editor = useEditor({
          extensions: [StarterKit],
          content: "<p>tiptap-probe</p>",
          immediatelyRender: false,
        });
        return h(EditorContent, { editor });
      }
      return h(Probe, null);
    },
    expect: (html) => html.startsWith("<div") && html.includes("</div>"),
  },
  {
    id: "fylun-excalidraw",
    label: "@excalidraw/excalidraw",
    why: "Fylun's whiteboard. A bundled React application shipped as one component: the whole module graph loads under the react→preact/compat aliasing, but the package reads `window` during module evaluation (top-level code in dist/prod), so importing it in a server render throws `window is not defined` before any component runs — the same thing happens under React SSR. Browser-only at import time; mount it as a client-only island.",
    load: async () => {
      const E = await import("@excalidraw/excalidraw");
      return h(E.Excalidraw, null);
    },
    expect: (html) => html.includes("excalidraw"),
  },
  {
    id: "fylun-xyflow",
    label: "@xyflow/react",
    why: "Fylun's node canvas (React Flow 12): d3-zoom, ResizeObserver and DOM measurement wrapped around a transformed node layer — all of it effect-side, so the wrapper, pane and nodes server-render.",
    load: async () => {
      const { ReactFlow } = await import("@xyflow/react");
      return h(ReactFlow, {
        nodes: [{ id: "n1", position: { x: 0, y: 0 }, data: { label: "flow-node" } }],
        edges: [],
        fitView: true,
      });
    },
    expect: (html) => html.includes("rf__wrapper") && html.includes("flow-node"),
  },
  {
    id: "fylun-monaco",
    label: "@monaco-editor/react",
    why: "Fylun's code editor. A thin wrapper: the server render legitimately produces only the wrapper section with its loading state — Monaco itself is fetched by @monaco-editor/loader on the client. The assertion targets that wrapper, which is all a correct server render of this component can produce. (No exports map: Node's CJS interop returns the whole exports object as `default`, hence the double `.default`.)",
    load: async () => {
      const mod = await import("@monaco-editor/react");
      const Editor = mod.default && mod.default.default ? mod.default.default : mod.default;
      return h(Editor, {
        height: "120px",
        defaultLanguage: "javascript",
        defaultValue: "const monacoProbe = 1;",
      });
    },
    expect: (html) => html.startsWith("<section") && html.includes("Loading..."),
  },
  {
    id: "fylun-cmdk",
    label: "cmdk",
    why: "Fylun's command palette (built on Radix Dialog internally), with input, list, empty state and item all in the tree.",
    load: async () => {
      const { Command } = await import("cmdk");
      return h(
        Command,
        null,
        h(Command.Input, { placeholder: "cmdk-input" }),
        h(
          Command.List,
          null,
          h(Command.Empty, null, "cmdk-empty"),
          h(Command.Item, { value: "cmdk-item" }, "cmdk-item")
        )
      );
    },
    expect: (html) => html.includes("cmdk-input") && html.includes("cmdk-item"),
  },
  {
    id: "fylun-sonner",
    label: "sonner",
    why: "Fylun's toasts. A correct server render is the live-region container itself (the labeled section with its aria attributes); toasts are client-side events, so the list is empty until hydration — that container is what the assertion checks.",
    load: async () => {
      const { Toaster } = await import("sonner");
      return h(Toaster, null);
    },
    expect: (html) => html.includes("Notifications") && html.includes("aria-live"),
  },
  {
    id: "fylun-react-markdown",
    label: "react-markdown",
    why: "Fylun's markdown rendering, with remark-gfm@4.0.1 in the plugin chain — the assertion asks for GFM output (a table and strikethrough), not just any markdown, so the plugin path is proven too.",
    load: async () => {
      const Markdown = (await import("react-markdown")).default;
      const remarkGfm = (await import("remark-gfm")).default;
      return h(
        Markdown,
        { remarkPlugins: [remarkGfm] },
        "| gfm_h1 | gfm_h2 |\n| --- | --- |\n| cell-a | cell-b |\n\n~~gfm-struck~~"
      );
    },
    expect: (html) => html.includes("<table") && html.includes("<del"),
  },
  {
    id: "fylun-lucide",
    label: "lucide-react",
    why: "Fylun's icon set; a `forwardRef` svg component per icon, mounted with an aria-label to check attribute passthrough.",
    load: async () => {
      const icons = await import("lucide-react");
      return h(icons.Rocket, { "aria-label": "lucide-rocket" });
    },
    expect: (html) => html.includes("<svg") && html.includes("lucide-rocket"),
  },
  {
    id: "fylun-qrcode",
    label: "qrcode.react",
    why: "Fylun's QR codes; the SVG encoder produces the full module matrix server-side.",
    load: async () => {
      const { QRCodeSVG } = await import("qrcode.react");
      return h(QRCodeSVG, { value: "qrcode-probe", size: 96 });
    },
    expect: (html) => html.includes("<svg") && html.includes("viewBox"),
  },

  // The rest of Fylun's Radix set at its exact versions. Dialog, dropdown-menu,
  // select, tabs and tooltip are already covered above — their pins were moved
  // to Fylun's exact versions (1.1.23 / 2.1.24 / 2.3.7 / 1.1.21 / 1.2.16) in
  // compat-matrix/package.json, and the version column now reports the
  // installed truth, so those existing rows are the rows for these packages.
  {
    id: "fylun-radix-checkbox",
    label: "@radix-ui/react-checkbox",
    why: "Fylun's checkbox at 1.3.11: controlled checked state with an Indicator child and the hidden native input fallback.",
    load: async () => {
      const C = await import("@radix-ui/react-checkbox");
      return h(C.Root, { checked: true, name: "fylun-checkbox" }, h(C.Indicator, null, "✓"));
    },
    expect: (html) => html.includes('role="checkbox"') && html.includes('aria-checked="true"'),
  },
  {
    id: "fylun-radix-switch",
    label: "@radix-ui/react-switch",
    why: "Fylun's switch at 1.3.7: same button-with-hidden-input shape as checkbox, thumb included.",
    load: async () => {
      const S = await import("@radix-ui/react-switch");
      return h(S.Root, { checked: true, name: "fylun-switch" }, h(S.Thumb, null));
    },
    expect: (html) => html.includes('role="switch"') && html.includes('aria-checked="true"'),
  },
  {
    id: "fylun-radix-avatar",
    label: "@radix-ui/react-avatar",
    why: "Fylun's avatar at 1.2.6: no image source in the tree, so the fallback text is the server render.",
    load: async () => {
      const A = await import("@radix-ui/react-avatar");
      return h(A.Root, null, h(A.Fallback, null, "AV"));
    },
    expect: (html) => html.includes(">AV<"),
  },
  {
    id: "fylun-radix-label",
    label: "@radix-ui/react-label",
    why: "Fylun's label at 2.1.15: a `forwardRef` around a native element.",
    load: async () => {
      const L = await import("@radix-ui/react-label");
      return h(L.Root, { htmlFor: "fylun-label-for" }, "fylun-label");
    },
    expect: (html) => html.startsWith("<label") && html.includes('for="fylun-label-for"'),
  },
  {
    id: "fylun-radix-progress",
    label: "@radix-ui/react-progress",
    why: "Fylun's progress at 1.1.16: aria state derived from a controlled value, indicator child in the tree.",
    load: async () => {
      const P = await import("@radix-ui/react-progress");
      return h(P.Root, { value: 42 }, h(P.Indicator, { style: { width: "42%" } }));
    },
    expect: (html) => html.includes('role="progressbar"') && html.includes('aria-valuenow="42"'),
  },
  {
    id: "fylun-radix-separator",
    label: "@radix-ui/react-separator",
    why: "Fylun's separator at 1.1.15: non-decorative, so the accessibility role must be present in the server output.",
    load: async () => {
      const S = await import("@radix-ui/react-separator");
      return h(S.Root, { decorative: false });
    },
    expect: (html) => html.includes('role="separator"') && html.includes("data-orientation"),
  },
  {
    id: "fylun-radix-slot",
    label: "@radix-ui/react-slot",
    why: "Fylun's slot at 1.3.3 (the base of its design-system composition): the child element must come through with the slot's props merged onto it.",
    load: async () => {
      const SlotNS = await import("@radix-ui/react-slot");
      const Slot = SlotNS.Slot ?? SlotNS.Root ?? SlotNS.default;
      return h(Slot, { className: "slot-probe" }, h("button", { type: "button" }, "slot-child"));
    },
    expect: (html) => html.startsWith("<button") && html.includes("slot-probe") && html.includes("slot-child"),
  },
];
