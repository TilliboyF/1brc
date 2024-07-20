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

  string() {
    return (
      this.min / 10 + "/" + this.sum / this.amount / 10 + "/" + this.max / 10
    );
  }
}
