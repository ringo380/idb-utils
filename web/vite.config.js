import { defineConfig } from 'vite';
import wasm from 'vite-plugin-wasm';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [wasm(), tailwindcss()],
  base: process.env.BASE_URL || '/',
  server: {
    fs: {
      // The wasm-pack output lives in ../pkg, outside this Vite root, so the
      // dev server refuses to serve idb_bg.wasm (403) without this. Only
      // affects `npm run dev`; the production build inlines the module.
      allow: ['..'],
    },
  },
  build: {
    target: 'esnext',
  },
});
