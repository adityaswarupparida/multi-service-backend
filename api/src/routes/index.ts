import { Router } from "express";
import uploadRouter from "./upload.js";

const router: Router = Router();

router.use("/records", uploadRouter);

export default router;