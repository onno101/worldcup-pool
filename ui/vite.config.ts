import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const devToken = env.VITE_DEV_ACCESS_TOKEN || "";
  // Set for subpath hosting (e.g. Databricks Apps) if the gateway URL is known at build time:
  //   VITE_PUBLIC_BASE=/workspace/.../ VITE_DEV_ACCESS_TOKEN=... npm run build
  const publicBase = env.VITE_PUBLIC_BASE || "/";
  return {
    base: publicBase,
    plugins: [react()],
    server: {
      port: 5173,
      proxy: {
        "/api": {
          target: "http://127.0.0.1:8000",
          changeOrigin: true,
          configure(proxy) {
            proxy.on("proxyReq", (proxyReq) => {
              if (devToken) {
                proxyReq.setHeader("x-forwarded-access-token", devToken);
              }
            });
          },
        },
      },
    },
    build: {
      outDir: "dist",
      emptyOutDir: true,
    },
  };
});
