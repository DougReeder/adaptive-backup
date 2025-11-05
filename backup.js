#!/usr/bin/env node

import { program /*, InvalidArgumentError*/ } from "commander";
import pkg from "./package.json"  with { type: "json" };
import process from "node:process";
import colors from "colors";
import prompt from "prompt";
import addQueryParamsToURL from "./src/addQueryParamsToURL.js"
import WebFinger from "webfinger.js";
import opener from "opener";
import {Backup} from "./src/backupClass.js";
import {errToMessage} from "./src/errToMessage.js";

if (!process.env.NODE_DEBUG) {
  console.debug = () => {};
}

function stringWithoutSlashes(value, _) {
  value = value.replace(/\//g, '');
  // if (/^public$/i.test(value)) {
  //   throw new InvalidArgumentError("Category may not be “public”");
  // }
  return value;
}

program
    .version(pkg.version)
    .requiredOption('-o, --backup-dir <path>', 'backup directory path')
    .option('-u, --user-address <user address>', 'user address (user@host)')
    .option('-t, --token <token>', 'valid bearer token')
    .option('-c, --category <category>', 'category (base directory) to back up', stringWithoutSlashes, '')
    .option('-p, --include-public', 'when backing up a single category, include the public folder of that category', false)
    .option('-s, --simultaneous <limit>', 'number of simultaneous connections', 9)

program.parse(process.argv);
const options = program.opts();
console.debug(colors.gray("options:", options));

const CLIENT_ID = 'adaptivebackup.hominidsoftware.com';
const ORIGIN = 'https://' + CLIENT_ID;
const authScope     = options.category.length > 0 ? options.category+':rw' : '*:rw';

let storageEndpoint;

const schemas = {
  userAddress: {
    name: 'userAddress',
    description: 'User address (user@host):',
    type: 'string',
    pattern: /^.+@.+$/,
    message: 'Please provide a valid user address. Example: tony@5apps.com',
    required: true
  },
  token: {
    name: 'token',
    description: 'Authorization token:',
    type: 'string',
    required: true
  }
};

const webfinger = new WebFinger({
  tls_only: true,   // Security-first: HTTPS only
  uri_fallback: false,
  // request_timeout: 10000  // 10 second timeout
});

// start
prompt.message = '';
prompt.delimiter = '';
prompt.override = program;
prompt.start();

let userAddress = options.userAddress;
let authEndpoint;
do {
  try {
    if (!userAddress) {
      const userInput = await prompt.get(schemas.userAddress);
      userAddress = userInput.userAddress;
    }

    const wfResult = await webfinger.lookup(userAddress);
    const link = wfResult.object?.links?.find?.(l => "http://tools.ietf.org/id/draft-dejong-remotestorage" === l.rel)
    authEndpoint = link?.properties?.["auth-endpoint"] || link?.properties?.["http://tools.ietf.org/html/rfc6749#section-4.2"];
    storageEndpoint = '/' === link.href.slice(-1) ? link.href : link.href + '/';
  } catch (err) {
    if ("canceled" === err.message) { process.exit(1); }
    console.error(colors.red(`WebFinger for “${userAddress}” failed:`, errToMessage(err)));
    userAddress = '';
  }
} while (!storageEndpoint);
console.debug(colors.gray('authorization endpoint:', authEndpoint));
console.debug(colors.gray('storage endpoint:', storageEndpoint));

if (!options.token) {
  console.info(colors.cyan('No auth token set via options. A browser window will open to connect your account.'));
  const authURL = addQueryParamsToURL(authEndpoint, {
    client_id: CLIENT_ID,
    redirect_uri: ORIGIN + '/',
    response_type: 'token',
    scope: authScope
  });
  opener(authURL);

  const tokenResult = await prompt.get(schemas.token);
  options.token = tokenResult.token;
}

try {
  const backup = new Backup(ORIGIN, options, storageEndpoint, pkg.version);
  backup.execute();

  process.on('SIGINT', handleInterruption);
  process.on('SIGTERM', handleInterruption);
  process.on('SIGQUIT', handleInterruption);
  process.on('SIGHUP', handleInterruption);

  function handleInterruption(signal) {
      console.error(colors.red(`Received ${signal}`));
      backup.abandonGracefully();

      setTimeout(() => {
        console.error(colors.red(`Exiting abruptly. These downloads are probably incomplete:`));
        const incomplete = Array.from(backup.queue.keys()).join("\n");
        console.error(colors.red(incomplete));
        process.exit(3);
      }, 10_000)
  }
} catch (err) {
  console.error("setting up: " + colors.red(errToMessage(err)));
}
