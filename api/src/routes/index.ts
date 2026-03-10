import { Router } from "express";
import uploadRouter from "./upload.js";

const router: Router = Router();

router.use("/upload", uploadRouter);
// router.use("/fetch", fetchRouter);

export default router;