// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  integrations: [
    starlight({
      title: "PgWorkflows",
      social: [{ icon: "github", label: "GitHub", href: "https://github.com/EDRasmussen/PgWorkflows" }],
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Get started", slug: "get-started" },
            { label: "How it works", slug: "how-it-works" },
          ],
        },
        {
          label: "Features",
          items: [
            { label: "Workflows & activities", slug: "workflows-and-activities" },
            { label: "Starting workflows", slug: "starting-workflows" },
            { label: "Workers & scaling", slug: "workers-and-scaling" },
            { label: "Fan-in fan-out", slug: "fan-in-fan-out" },
            { label: "Sleep", slug: "sleep" },
            { label: "Signals", slug: "signals" },
            { label: "Error handling", slug: "error-handling" },
            { label: "Observability", slug: "observability" },
          ],
        },
        {
          label: "Reference",
          items: [{ autogenerate: { directory: "reference" } }],
        },
      ],
    }),
  ],
});
