const json = JSON.stringify;

class EventLog {
  s3(type, arg) {
    if (!this.enabled_) return;
    const caller = this.callsite(null, [
      /^EventLog/,
    ]).summary;
    console.debug(`${json(caller)}: S3 ${type}: ${json(arg).substr(0, 2048)}`);
  }
}

module.exports = EventLog;
