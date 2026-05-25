const fs = require('fs');
const path = require('path');
const winston = require('winston');

const LOGS_DIR = path.join(__dirname, '..', 'logs');

if (!fs.existsSync(LOGS_DIR)) {
  fs.mkdirSync(LOGS_DIR);
}

function createLogger(filename) {

  return winston.createLogger({
    level: 'debug',

    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.simple()
    ),

    transports: [
      new winston.transports.File({
        filename: path.join(LOGS_DIR, filename)
      }),
      new winston.transports.Console()
    ]
  });
}

module.exports = createLogger;