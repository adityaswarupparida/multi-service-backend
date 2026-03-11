-- AlterEnum
ALTER TYPE "Status" ADD VALUE 'REPLACED';

-- AlterTable
ALTER TABLE "Upload" ADD COLUMN     "checksum" TEXT;
