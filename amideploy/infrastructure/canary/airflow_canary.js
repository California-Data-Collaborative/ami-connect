const synthetics = require('Synthetics');
const log = require('SyntheticsLogger');
const http = require('http');

const ACCEPTABLE_STATUS_CODES = new Set([200, 302]);

const canaryTest = async function () {
  const url = `http://${process.env.AIRFLOW_HOSTNAME}/`;

  await new Promise((resolve, reject) => {
    log.info('Checking URL: ' + url);
    const req = http.get(url, res => {
      log.info('Status Code: ' + res.statusCode);
      if (ACCEPTABLE_STATUS_CODES.has(res.statusCode)) {
        resolve();
      } else {
        reject(`Non-200 response: ${res.statusCode}`);
      }
    });
    req.on('error', reject);
  });
};

exports.handler = async () => {
  await canaryTest();
};
