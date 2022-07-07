"use strict";

const fs = require('fs');
const request = require('request');
const crypto = require('crypto');
const Web3 = require('web3');
const os = require("os");
const sort = require('fast-sort').sort;

const DINGO_COOKIE_PATH = '~/.dingocoin/.cookie'.replace('~', os.homedir);
const DINGO_PORT = 34646;

module.exports = {
  toSatoshi,
  fromSatoshi,
  walletPassphrase,
  verifyAddress,
  getClientVersion,
  getBlockchainInfo,
  getTxOutSetInfo,
  getBlockHash,
  getTransaction,
  getNewAddress,
  createMultisig,
  importAddress,
  listReceivedByAddress,
  getReceivedAmountByAddress,
  getReceivedAmountByAddresses,
  listUnspent,
  decodeRawTranscation,
  createRawTransaction,
  signRawTransaction,
  verifyRawTransaction,
  sendRawTranscation
};

function toSatoshi(x) {
  if (x === null || x === undefined || typeof(x) !== 'string' || x === '') {
    throw new Error('Expected string input');
  }
  return (BigInt(Web3.utils.toWei(x, 'gwei')) / 10n).toString();
}

function fromSatoshi(x) {
  if (x === null || x === undefined || typeof(x) !== 'string' || x === '') {
    throw new Error('Expected string input');
  }
  return (Web3.utils.fromWei((BigInt(x) * 10n).toString(), 'gwei')).toString();
}

function getCookie() {
  const data = fs.readFileSync(DINGO_COOKIE_PATH, 'utf-8').split(':');
  return {
    user: data[0],
    password: data[1]
  };
}

async function callRpc(method, params) {
  const cookie = getCookie();
  const options = {
    url: "http://localhost:" + DINGO_PORT.toString(),
    method: "post",
    headers: {
      "content-type": "text/plain"
    },
    auth: {
      user: cookie.user,
      pass: cookie.password
    },
    body: JSON.stringify({
      "jsonrpc": "1.0",
      "method": method,
      "params": params
    })
  };

  return new Promise((resolve, reject) => {
    request(options, (err, resp, body) => {
      if (err) {
        return reject(err);
      } else {
        const r = JSON.parse(body
          .replace(/"(amount|value)":\s*(\d+)\.((\d*?[1-9])0*),/g, '"$1":"$2\.$4",')
          .replace(/"(amount|value)":\s*(\d+)\.0+,/g, '"$1":"$2",'));
        if (r.error) {
          reject(r.error.message);
        } else {
          resolve(r.result);
        }
      }
    });
  });
}

async function verifyAddress(address) {
  return (await callRpc('validateaddress', [address])).isvalid;
}

async function getClientVersion() {
  return (await callRpc('getinfo', [])).version;
}

function getBlockchainInfo() {
  return callRpc('getblockchaininfo', []);
}

function getTxOutSetInfo() {
  return callRpc('gettxoutsetinfo', []);
}

function getBlockHash(height) {
  return callRpc('getblockhash', [height]);
}

function walletPassphrase(passphrase) {
  return callRpc('walletpassphrase', [passphrase, 1000000]);
}

function getTransaction(hash) {
  return callRpc('gettransaction', [hash]);
}

async function getNewAddress() {
  return (await callRpc('validateaddress', [await callRpc('getnewaddress', [])])).pubkey;
}

function createMultisig(n, individualAddresses) {
  return callRpc('createmultisig', [n, individualAddresses]);
}

async function importAddress(redeemScript) {
  return callRpc('importaddress', [redeemScript, '', false, true]);
}

async function listReceivedByAddress(confirmations) {
  const data = await callRpc('listreceivedbyaddress', [confirmations, false, true]);
  const dict = {};
  for (const entry of data) {
    dict[entry.address] = entry;
  }
  return dict;
}

async function getReceivedAmountByAddress(confirmations, address) {
  const received = await listReceivedByAddress(confirmations);
  if (!(address in received)) {
    return 0;
  }
  return received[address].amount;
}

async function getReceivedAmountByAddresses(confirmations, addresses) {
  const received = await listReceivedByAddress(confirmations);
  const result = {};
  for (const address of addresses) {
    if (!(address in received)) {
      result[address] = 0;
    } else {
      result[address] = received[address].amount;
    }
  }
  return result;
}

function listUnspent(confirmations, addresses) {
  if (addresses === null || addresses === undefined || addresses.length === 0) {
    return [];
  } else {
    return callRpc('listunspent', [confirmations, 9999999, addresses]);
  }
}

function decodeRawTranscation(hex) {
  return callRpc('decoderawtransaction', [hex]);
}

function createRawTransaction(unspent, payouts) {
  return callRpc('createrawtransaction', [unspent, payouts]);
}

async function signRawTransaction(hex) {
  return (await callRpc('signrawtransaction', [hex])).hex;
}

async function verifyRawTransaction(unspent, payouts, hex) {
  const tx = await decodeRawTranscation(hex);
  if (tx.vin.length !== unspent.length) {
    throw new Error('Unspent mismatch');
  }

  const proposedVins = sort(unspent).asc((x) => x.txid + x.vout.toString()).map((x) => [x.txid, x.vout]);
  const txVins = sort(tx.vin).asc((x) => x.txid + x.vout.toString()).map((x) => [x.txid, x.vout]);
  if (JSON.stringify(proposedVins) !== JSON.stringify(txVins)) {
    throw new Error('Unspent mismatch');
  }

  if (tx.vout.length !== Object.keys(payouts).length) {
    throw new Error('Payouts mismatch');
  }
  const proposedVouts = sort(Object.keys(payouts)).asc().map((x) => [x, payouts[x]]);
  const txVouts = sort(tx.vout.slice()
      .filter((x) => (x.scriptPubKey.type === 'scripthash' || x.scriptPubKey.type === 'pubkeyhash') && x.scriptPubKey.addresses.length === 1)
      .map((x) => [x.scriptPubKey.addresses[0], x.value.toString()]))
    .asc((x) => x[0]);

  const proposedVoutsMap = new Map(proposedVouts);
  const txVoutsMap = new Map(txVouts);
  if (Object.keys(proposedVoutsMap).length !== Object.keys(txVoutsMap).length) {
    throw new Error('Payouts mistmatch in length');
  }
  if (!txVouts.every((x) => proposedVoutsMap.has(x[0]) && toSatoshi(proposedVoutsMap.get(x[0])) === toSatoshi(x[1]))) {
    throw new Error('Payouts mistmatch in content');
  }
  if (!proposedVouts.every((x) => txVoutsMap.has(x[0]) && toSatoshi(txVoutsMap.get(x[0])) === toSatoshi(x[1]))) {
    throw new Error('Payouts mistmatch in content');
  }
}

function sendRawTranscation(hex) {
  return callRpc('sendrawtransaction', [hex]);
}
