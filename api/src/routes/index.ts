import { Router } from "express";
import recordRouter from "./records.js";

const router: Router = Router();

router.use("/records", recordRouter);

export default router;