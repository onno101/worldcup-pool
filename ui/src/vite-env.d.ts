/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_DEV_ACCESS_TOKEN?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
