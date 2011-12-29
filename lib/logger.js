var streamLogger = require('./streamlogger');

exports.logger = function(logfile) {
  var _logger = new streamLogger.StreamLogger(logfile || '/dev/stdout');
  _logger.level = _logger.levels.debug;

  process.on('SIGUSR2', function() {
    _logger.reopen(function() {
      console.log('log file reopened!');
    });
  });
  return { info : function(msg) {
    _logger.info('[' + process.pid + ']' + msg);
  }, debug : function(msg) {
    _logger.debug('[' + process.pid + ']' + msg);
  } };
};