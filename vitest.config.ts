import { defineConfig } from 'vitest/config';
import { loadEnv } from 'vite';

export default defineConfig(({ mode }) => {
  // Load env file from current directory
  const env = loadEnv(mode, process.cwd(), '');

  return {
    test: {
      testTimeout: 300000, // 5 min - DO jobs can take longer
      hookTimeout: 60000,
      env: env,

      // Single thread to avoid race conditions
      pool: 'forks',
      poolOptions: {
        forks: {
          singleFork: true,
        },
      },
    },
  };
});
