#!/usr/bin/node

// Script that generates test data for use with net.sansa_stack.hadoop.RecordReaderJsonArrayTest
// The only argument is the size of the array which to generate
// Example usage: generate-test-json-array.js 1000

const myArgs = process.argv.slice(2);

const n = parseInt(myArgs[0])

console.log('[')
for (let i = 0; i < n; ++i) {

  let obj = {};
  obj.id = i;

  obj.arr = [];
  let m = Math.trunc(Math.random() * 100)
  for (let j = 0; j < m; ++j) {
    obj.arr.push(j)
  }

  console.log(JSON.stringify(obj)); //, null, 2))

  if (i + 1 != n) {
    console.log(',')
  }

}
console.log(']')
