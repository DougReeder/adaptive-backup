import { getSystemErrorName } from 'node:util';

/**
 * Extracts & de-dupes payloads of error tree, for terse logging
 * @param {Error} err
 * @param {Set} messages
 * @returns {string} the message
 */
export function errToMessage(err, messages = new Set()) {
  try {
    if (!(err instanceof Object)) { return Array.from(messages).join(": "); }

    if (err.name !== 'AggregateError') {
      if (err.name && !err.message?.includes(err.name) &&
        !Array.from(messages).some(msg => typeof msg === 'string' && msg?.includes(err.name))) {
        messages.add(err.name);
      }
      if (err.message) {
        messages.add(err.message?.replace(/\r\n|\n|\r/, ' '));
      }
      if (err.code && !Array.from(messages).some(msg => typeof msg === 'string' && msg?.includes(err.code))) {
        messages.add(err.code);
      }
      const errno = err.errno ? getSystemErrorName(err.errno) : '';
      if (errno && !Array.from(messages).some(msg => typeof msg === 'string' && msg?.includes(errno))) {
        messages.add(errno);
      }
    }
    if (err.errors?.[Symbol.iterator]) {
      for (const e of err.errors) {
        errToMessage(e, messages);
      }
    }
    if (err.cause) {
      errToMessage(err.cause, messages);
    }
  } catch (err2) {
    messages.add(err2);
  }
  return Array.from(messages).join(": ");
}
