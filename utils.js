const path = require("path");
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

const MAX_BIT_LENGTH = 32;
/** Compute a hash of desired length for the input string.
 * The function uses SHA-1 to compute an intermmediate string output,
 * then truncates to the user-specified size from the high-order bits.
 *
 * @param {string} stringForHashing
 * @param {number} hashBitLength
 * @returns {number}
 */
async function computeIntegerHash(stringForHashing, hashBitLength) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  const MAX_JS_INT_BIT_LENGTH = 32;
  const BIT_PER_HEX_CHARACTER = 4;
  // sanitize length input
  if (hashBitLength > MAX_BIT_LENGTH) {
    hashBitLength = MAX_BIT_LENGTH;
    console.log(
      `Warning. Requested ${hashBitLength} bits `,
      `but only ${MAX_BIT_LENGTH} bits available due to numerical simplification.`,
      `Thus, using only ${hashBitLength} bits.`,
      `In computeHash().`
    );
  }
  let hashOutput = await sha1(stringForHashing);
  if (DEBUGGING_LOCAL) {
    console.log(`Full hash of "${stringForHashing}" is ${hashOutput}.`);
  }
  /* JavaScript only does bitwise operations on 32-bit numbers
     so keep only the top 32 bits of the hashed value.
  */
  hashOutput = hashOutput.slice(
    0,
    MAX_JS_INT_BIT_LENGTH / BIT_PER_HEX_CHARACTER
  );
  if (DEBUGGING_LOCAL) {
    console.log(`Truncated string value is ${hashOutput}.`);
  }
  // convert from hexadecimal to decimal
  integerHash = parseInt("0x" + hashOutput);
  if (DEBUGGING_LOCAL) {
    console.log(`Integer value is ${integerHash}.`);
  }
  // truncate the hash to the desired number of bits by picking the high-order bits
  integerHash = integerHash >>> (MAX_BIT_LENGTH - hashBitLength);
  if (DEBUGGING_LOCAL) {
    console.log(`Truncated integer value is ${integerHash}.`);
  }
  return integerHash;
}

module.exports = {
  isInModuloRange,
  computeIntegerHash,
  sha1
};
