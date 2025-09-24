#!/usr/bin/env node

import {program} from "commander";
import pkg from "./package.json"  with { type: "json" };
import process from "node:process";
import colors from "colors";
import prompt from "prompt";
import addQueryParamsToURL from "./addQueryParamsToURL.js"
import WebFinger from "webfinger.js";
import opener from "opener";
import RemoteStorage from 'remotestoragejs';

program
    .version(pkg.version)
    .option('-o, --backup-dir <path>', 'backup directory path')
    .option('-u, --user-address <user address>', 'user address (user@host)')
    .option('-t, --token <token>', 'valid bearer token')
    .option('-c, --category <category>', 'category (base directory) to back up')
    .option('-p, --include-public', 'when backing up a single category, include the public folder of that category')

program.parse(process.argv);
const options = program.opts();
console.log("options:", options);

const CLIENT_ID = 'throttled-backup.hominidsoftware.com';
const ORIGIN = 'https://' + CLIENT_ID;
const backupDir     = options.backupDir;
const category      = options.category || '';
const includePublic = options.includePublic || false;
const authScope     = category.length > 0 ? category+':rw' : '*:rw';

let userAddress     = options.userAddress;
let token           = options.token;


async function executeBackup() {
  throw new Error("not implemented");
}



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
if (!(token && userAddress)) {
  console.info(colors.cyan('No user address and/or auth token set via options. A browser window will open to connect your account.'));
  prompt.message = '';
  prompt.delimiter = '';
  prompt.override = program;
  prompt.start();

  let endpoint;
  do {
    try {
      const userInput = await prompt.get(schemas.userAddress);
      userAddress = userInput.userAddress;

      const wfResult = await webfinger.lookup(userAddress);
      const link = wfResult.object?.links?.find?.(l => "http://tools.ietf.org/id/draft-dejong-remotestorage" === l.rel)
      endpoint = link?.properties?.["auth-endpoint"] || link?.properties?.["http://tools.ietf.org/html/rfc6749#section-4.2"];
    } catch (err) {
      console.error(colors.red(err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err));
    }
  } while (!endpoint);

  console.log('Remote storage endpoint:', endpoint);

   const authURL = addQueryParamsToURL(endpoint, {
    client_id: CLIENT_ID,
    redirect_uri: ORIGIN + '/',
    response_type: 'token',
    scope: authScope
  });

  opener(authURL);

  const tokenResult = await prompt.get(schemas.token);
  token = tokenResult.token;
}
try {
  await executeBackup();
} catch (err) {
  console.error(colors.red(err.message || err.cause?.message || err.code || err.cause?.code || err.errno || err.cause?.errno || err));
}
