const { Worker } = require("worker_threads");

/**
 * Accounts for the modulo arithmetic to determine whether the input value is within the bounds.
 * Implements inclusive and exclusive properties for each bound, as specified.
 *
 *      USAGE
 *      includeLower == true means [lowerBound, ...
 *      includeLower == false means (lowerBound, ...
 *      includeUpper == true means ..., upperBound]
 *      includeUpper == false means ..., upperBound)
 *
 * @param {number} inputValue
 * @param {number} lowerBound
 * @param {boolean} includeLower
 * @param {number} upperBound
 * @param {boolean} includeUpper
 * @returns {boolean} true if the input value is in - modulo - bounds; false otherwise
 */
function isInModuloRange(
  inputValue,
  lowerBound,
  includeLower = true,
  upperBound,
  includeUpper = false
) {
  if (includeLower && includeUpper) {
    if (lowerBound > upperBound) {
      //looping through 0
      return inputValue >= lowerBound || inputValue <= upperBound;
    } else {
      return inputValue >= lowerBound && inputValue <= upperBound;
    }
  } else if (includeLower && !includeUpper) {
    if (lowerBound > upperBound) {
      //looping through 0
      return inputValue >= lowerBound || inputValue < upperBound;
    } else {
      return inputValue >= lowerBound && inputValue < upperBound;
    }
  } else if (!includeLower && includeUpper) {
    if (lowerBound > upperBound) {
      //looping through 0
      return inputValue > lowerBound || inputValue <= upperBound;
    } else {
      return inputValue > lowerBound && inputValue <= upperBound;
    }
  } else {
    if (lowerBound > upperBound) {
      //looping through 0
      return inputValue > lowerBound || inputValue < upperBound;
    } else {
      return inputValue > lowerBound && inputValue < upperBound;
    }
  }
}

/**
 * Creates a worker thread to execute crypto and returns a result.
 * To use `const result = await sha1("stuff2");`
 * @param {string} source
 * @returns {Promise} - Resolves to the SHA-1 hash of source
 */
function sha1(source) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(path.join(__dirname, "./cryptoThread.js"), {
      workerData: source
    });
    worker.on("message", resolve);
    worker.on("error", reject);
  });
}

module.exports = {
  isInModuloRange,
  sha1
};
