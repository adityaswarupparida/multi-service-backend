import express from "express";
import apiRouter from "./routes/index.js";

const app = express();
const PORT = 3000;

app.use(express.json());

app.use("/api", apiRouter);

app.get("/health", (req, res) => {
    res.status(200).json({ message: "OK" });
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});