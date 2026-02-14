import { defineConfig } from 'vite';
import wasm from 'vite-plugin-wasm';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [wasm(), tailwindcss()],
  base: '/idb-utils/',
  build: {
    target: 'esnext',
  },
});
