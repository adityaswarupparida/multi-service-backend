import { defineConfig } from 'vitest/config';
import { config } from 'dotenv';

config();

export default defineConfig({
    test: {
        environment: 'node',
        include: ['src/tests/**/*.test.ts'],
        testTimeout: 30000,
        globals: true,
        server: {
            deps: {
                external: [/@prisma/]
            }
        },
        coverage: {
            provider: 'v8',
            reporter: ['text', 'html'],
            include: ['src/**/*.ts'],
            exclude: ['src/tests/**'],
        }
    },
});
