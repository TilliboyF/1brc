import fs from "node:fs";
import { performance } from "node:perf_hooks";
import {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} from "node:worker_threads";
import { queue } from "async";
import { cpus } from "node:os";

const wt = () => {
  const filePath = "path/to/your/file.txt";
  const chunkSize = 1024 * 1024;

  if (isMainThread) {
    const numWorkers = cpus().length - 1;

    const resultmap = new Map();

    const taskQueue = queue((chunk, callback) => {
      const worker = new Worker(__filename, { workerData: chunk });
      worker.on("message", (map) => {
        resultQueue.push(map, (err) => {
          if (err) console.error("Error processing result:", err);
        });
        callback();
      });
      worker.on("error", callback);
    }, numWorkers);

    const resultQueue = queue((map, callback) => {
      for (const [city, temp] of map.entries()) {
        if (resultmap.has(city)) {
          let m = resultmap.get(city);
          m.addMeasurement(temp);
        } else {
          resultmap.set(city, temp);
        }
      }
      callback();
    }, 1);
  } else {
    const chunk = workerData;
    const data = processChunk(chunk);
    parentPort.postMessage(data);
  }
};

/**
 *
 * @param {string} chunk
 * @returns {Map} result
 */
const processChunk = (chunk) => {
  const data = new Map();

  let lines = chunk.split("\n");

  lines.forEach((line) => {
    let parts = line.split(";");
    let city = parts[0];
    let temp = stringToInt(parts[1]);
    if (data.has(city)) {
      let m = data.get(city);
      m.addTemp(temp);
    } else {
      data.set(city, new Measurement(temp));
    }
  });

  return data;
};

const start = performance.now();

const fileStream = fs.createReadStream("../data/measurements.txt", {
  highWaterMark: 1024 * 1024, // 1MB buffer size
});

const lineReader = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity,
});

let data = new Map();

lineReader.on("line", (line) => {
  let parts = line.split(";");
  let city = parts[0];
  let temp = stringToInt(parts[1]);
  if (data.has(city)) {
    let m = data.get(city);
    m.addTemp(temp);
  } else {
    data.set(city, new Measurement(temp));
  }
});

lineReader.on("close", () => {
  data.entries();
  for (const [city, m] of data) {
    console.log(city + "=" + m.string());
  }
  const end = performance.now();
  console.log("took " + (end - start) / (60 * 1000) + "min");
});

/**
 *
 * @param {string} temp
 * @returns {int} result
 */
const stringToInt = (temp) => {
  let index = 0;
  let negativ = false;
  if (temp[index] == "-") {
    negativ = true;
    index++;
  }
  let result = +temp[index];
  index++;
  if (temp[index] != ".") {
    result = 10 * result + +temp[index];
    index++;
  }
  index++;
  result = 10 * result + +temp[index];

  if (negativ) {
    result = -result;
  }
  return result;
};

class Measurement {
  min = 0;
  max = 0;
  sum = 0;
  amount = 1;
  /**
   *
   * @param {int} temp
   */
  constructor(temp) {
    this.min = temp;
    this.max = temp;
    this.sum = temp;
  }

  /**
   *
   * @param {int} temp
   */
  addTemp(temp) {
    if (temp < this.min) {
      this.min = temp;
    }
    if (temp > this.max) {
      this.max = temp;
    }
    this.sum += temp;
    this.amount++;
  }

  /**
   * @param {Measurement} temp
   */
  addMeasurement(temp) {
    if (temp.min < this.min) {
      this.min = temp.min;
    }
    if (temp.max > this.max) {
      this.max = temp.max;
    }
    this.sum += temp.sum;
    this.amount += temp.amount;
  }

  string() {
    return (
      this.min / 10 + "/" + this.sum / this.amount / 10 + "/" + this.max / 10
    );
  }
}
