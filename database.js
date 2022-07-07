"use strict";

const childProcess = require('child_process');
const sqlite3 = require('sqlite3')
const util = require('util');

let db = null;
let dbLock = null;

module.exports = {
  dump,
  reset,
  load,

  hasUsedDepositAddresses,
  registerUsedDepositAddresses,

  registerMintDepositAddress,
  getMintDepositAddress,
  getMintDepositAddresses,
  updateMintDepositAddresses,

  registerWithdrawal,
  getWithdrawal,
  getWithdrawals,
  getUnapprovedWithdrawals,
  updateWithdrawals
};

async function dump(path) {
  return (await util.promisify(childProcess.exec)(`sqlite3 ${path} ".dump"`)).stdout;
}

async function reset(path, sql) {
  try {
    childProcess.execSync(`rm ${path}`, {
      stdio: 'ignore'
    });
  } catch (err) {}
  const child = childProcess.spawn(`sqlite3`);
  child.stdin.setEncoding('utf-8');
  child.stdin.write(`.open ${path}\n`);
  child.stdin.write(sql);
  child.stdin.write('\n');
  child.stdin.end();
}

function load(path) {
  db = new sqlite3.Database(path);
}

async function hasUsedDepositAddresses(depositAddresses) {
  return (await util.promisify(db.get.bind(db))(
    `SELECT COUNT(*) from usedDepositAddresses WHERE address IN (${depositAddresses.map(x => '?')})`,
    depositAddresses
  ))['COUNT(*)'] > 0;
}

async function registerUsedDepositAddresses(depositAddresses) {
  const statement = db.prepare('INSERT INTO usedDepositAddresses (address) VALUES (?)');
  for (const depositAddress of depositAddresses) {
    await util.promisify(statement.run.bind(statement))([depositAddress]);
  }
  statement.finalize();
}

function registerMintDepositAddress(mintAddress, depositAddress, redeemScript) {
  return util.promisify(db.run.bind(db))(
    'INSERT INTO mintDepositAddresses (mintAddress, depositAddress, redeemScript) VALUES (?, ?, ?)',
    [mintAddress, depositAddress, redeemScript]
  );
}

async function getMintDepositAddress(mintAddress) {
  const results = await util.promisify(db.all.bind(db))(
    'SELECT depositAddress, approvedAmount, approvedTax FROM mintDepositAddresses WHERE mintAddress=?',
    [mintAddress]
  );
  if (results.length === 0) {
    return null;
  }
  if (results.length !== 1) {
    throw new Error('Whoever wrote the SQL code is a noob');
  }
  return results[0];
}

function getMintDepositAddresses(filterDepositAddresses) {
  if (filterDepositAddresses !== null && filterDepositAddresses !== undefined) {
    return util.promisify(db.all.bind(db))(
      `SELECT mintAddress, depositAddress, approvedAmount, approvedTax FROM mintDepositAddresses WHERE depositAddress IN (${filterDepositAddresses.map(x => '?')})`,
      filterDepositAddresses
    );
  } else {
    return util.promisify(db.all.bind(db))(`SELECT mintAddress, depositAddress, approvedAmount, approvedTax FROM mintDepositAddresses`);
  }
}

// TODO: Maybe warn that only the approvedTax field will be updated.
async function updateMintDepositAddresses(mintDepositAddresses) {
  const stmt = db.prepare(`UPDATE mintDepositAddresses SET approvedAmount=?, approvedTax=? WHERE depositAddress=?`);
  for (const a of mintDepositAddresses) {
    await stmt.run(a.approvedAmount, a.approvedTax, a.depositAddress);
  }
  stmt.finalize();
}

function registerWithdrawal(withdrawal) {
  return util.promisify(db.run.bind(db))(
    'INSERT INTO withdrawals (burnSignature, burnAmount, burnDestination) VALUES (?, ?, ?)',
    [withdrawal.burnSignature, withdrawal.burnAmount, withdrawal.burnDestination]
  );
}

async function getWithdrawal(burnSignature) {
  const result = await util.promisify(db.all.bind(db))(
    `SELECT burnSignature, burnAmount, burnDestination, approvedAmount, approvedTax from withdrawals WHERE burnSignature=?`,
    [burnSignature]
  );
  if (result.length === 0) {
    return null;
  }
  if (result.length !== 1) {
    throw new Error('Withdrawal duplicated on (burnSignature)');
  }
  return result[0];
}

function getWithdrawals() {
  return util.promisify(db.all.bind(db))(
    `SELECT burnSignature, burnAmount, burnDestination, approvedAmount, approvedTax FROM withdrawals`
  );
}

function getUnapprovedWithdrawals() {
  return util.promisify(db.all.bind(db))(
    `SELECT burnSignature, burnAmount, burnDestination, approvedAmount, approvedTax FROM withdrawals WHERE approvedTax="0"`
  );
}

async function updateWithdrawals(withdrawals) {
  const stmt = db.prepare(`UPDATE withdrawals SET approvedAmount=?, approvedTax=? WHERE burnSignature=?`);
  for (const w of withdrawals) {
    await stmt.run(w.approvedAmount, w.approvedTax, w.burnSignature);
  }
  stmt.finalize();
}
