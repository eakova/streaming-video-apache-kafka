const express = require("express");
const app = express();
const port = 5000;
const path = require("path");
// const streamer = require("./kafka/stream");
const producer = require("./kafka/producer");
const consumer = require("./kafka/consumer");
const fs = require("fs");
var stream = require("stream");
var streamBuffers = require("stream-buffers");

var current = 0;
var mes = [];

app.get("/streaming", async (req, res) => {
  console.log("streaming");
  const range = req.headers.range;

  let readStream = new stream.PassThrough();

  const objs = mes;
  objs.sort((a, b) =>
    JSON.parse(a.key) > JSON.parse(b.key)
      ? 1
      : JSON.parse(b.key) > JSON.parse(a.key)
      ? -1
      : 0
  );

  const buf_array = objs.map((b) => b.value)[current];

  if (!buf_array) {
    return res.end();
  }

  const buf = Buffer(buf_array);

  current++;

  const start = 0;
  const end = buf.length;
  const size = end - start;
  const head = {
    "Access-Control-Allow-Origin": "*",
    // "Content-Range": `bytes ${start}-${end}/17839845`,
    "Accept-Ranges": "bytes",
    "Content-Length": size,
    "Content-Type": "video/mp4",
  };
  myReadableStreamBuffer = new streamBuffers.ReadableStreamBuffer({
    frequency: 10,
    chunkSize: 2,
  });
  res.writeHead(206, head);

  readStream.end(buf);
  readStream.pipe(res);
});

app.get("/file_streaming", (req, res) => {
  const path = "video.mp4";
  const stat = fs.statSync(path);
  const fileSize = stat.size;
  const range = req.headers.range;
  let start;
  let end;
  if (range) {
    const parts = range.replace(/bytes=/, "").split("-");
    start = parseInt(parts[0], 10);
    end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;
  } else {
    start = 0;
    end = 1;
  }

  const chunksize = end - start + 1;
  const file = fs.createReadStream(path, { start, end });
  const head = {
    "Access-Control-Allow-Origin": "*",
    "Content-Range": `bytes ${start}-${end}/${fileSize}`,
    "Accept-Ranges": "bytes",
    "Content-Length": chunksize,
    "Content-Type": "video/mp4",
  };
  res.writeHead(206, head);
  file.pipe(res);
});

app.get("/produce", async (req, res) => {
  producer();
  const cons = await consumer();
  cons.run({
    eachMessage: ({ topic, partition, message }) => {
      console.log({
        value: message.value,
        key: JSON.parse(message.key),
      });
      const new_message = Object.assign({}, message);
      new_message_key = JSON.parse(message.key);
      req;
      const start = 0;
      const end = message.value.length;
      const chunksize = end - start + 1;
      mes.push(new_message);
    },
  });
  res.setHeader("Content-type", "application/json");
  res.setHeader("Access-Control-Allow-Origin", "*");

  res.send({ message: "producing" });
});
app.get("/consume", async (req, res) => {
  consumer();
  res.setHeader("Content-type", "application/json");
  res.setHeader("Access-Control-Allow-Origin", "*");

  res.send({ message: "producing" });
});

app.get("/", function (req, res) {
  res.sendFile(path.resolve("index.html"));
});
app.listen(port, () =>
  console.log(`Example app listening at http://localhost:${port}`)
);
