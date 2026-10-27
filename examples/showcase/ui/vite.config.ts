import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// `npm run dev` proxies the API to a local `showcase ui` (SHOWCASE_UI_ADDR).
export default defineConfig({
  plugins: [react()],
  // Keep the bundled packages' license comments (React is MIT).
  esbuild: { legalComments: "eof" },
  build: { outDir: "../web/dist", emptyOutDir: true },
  server: { proxy: { "/api": "http://127.0.0.1:8090" } },
  test: { environment: "node" },
});
