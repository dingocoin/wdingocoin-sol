'use strict';

const web3 = require('@solana/web3.js');
const nacl = require('tweetnacl');
const fs = require('fs');
const bs58 = require('bs58')
const splToken = require('@solana/spl-token');
const util = require('util');
const exec = util.promisify(require('child_process').exec);

module.exports = {
  isAddress,
  load,
  getWalletAddress,
  createSignedMessage,
  validateSignedMessage,
  validateSignedMessageOne,
  hasTokenAccount,
  signMint,
  finalizeMintAndSend,
  getBurn
};

let settings = null;
let connection = null;
let wallet = null;

function load(_settings) {
  settings = _settings;
  connection =
    new web3.Connection(web3.clusterApiUrl(settings.cluster), 'confirmed');
  wallet = web3.Keypair.fromSecretKey(
    new Uint8Array(JSON.parse(fs.readFileSync(settings.keypairPath))));
}

function isAddress(address) {
  if (typeof address !== 'string') {
    return false;
  }
  const x = bs58.decodeUnsafe(address);
  if (typeof x === 'undefined') {
    return false;
  }
  if (x.length !== 32) {
    return false;
  }
  return true;
}

function getWalletAddress() {
  return wallet.publicKey.toBase58();
}

// Expects string message. Returns hex.
function sign(message) {
  return Buffer
    .from(nacl.sign.detached(Buffer.from(message, 'utf-8'), wallet.secretKey))
    .toString('hex');
}

// Expects string message, base58 signature, base58 public key.
function verify(message, signature, address) {
  return nacl.sign.detached.verify(Buffer.from(message, 'utf-8'),
    Buffer.from(signature, 'hex'),
    bs58.decode(address));
}

function isSpecified(x) {
  return x !== undefined && x !== null;
}

function createSignedMessage(data) {
  return {
    data: data,
    signature: sign(JSON.stringify(data))
  };
}

function validateSignedMessageStructure(message) {
  if (!isSpecified(message)) {
    throw new Error('Message not specified');
  }
  if (isSpecified(message.error)) {
    throw new Error(message.error);
  }
  if (!isSpecified(message.data)) {
    throw new Error('Message missing data');
  }
  if (!isSpecified(message.signature) ||
    typeof message.signature !== 'string') {
    throw new Error('Message missing signature');
  }
}

function validateSignedMessage(message, walletAddress, discard = true) {
  if (!verify(JSON.stringify(message.data), message.signature, walletAddress)) {
    throw new Error('Authority verification failed');
  }
  if (discard) {
    return message.data;
  } else {
    return message;
  }
}

function validateSignedMessageOne(message, walletAddresses, discard = true) {
  validateSignedMessageStructure(message);
  const verifications = walletAddresses.map(
    x => verify(JSON.stringify(message.data), message.signature, x) ? 1 : 0);
  if (verifications.reduce((a, b) => a + b, 0) !== 1) {
    throw new Error('Authority verification failed');
  }
  if (discard) {
    return message.data;
  } else {
    return message;
  }
}

async function hasTokenAccount(address) {
  const accounts = await connection.getTokenAccountsByOwner(new web3.PublicKey(address), {
    mint: new web3.PublicKey(settings.tokenAddress)
  });
  return accounts.value.length > 0;
}

function getTokenAccount(address) {
  return splToken.Token.getAssociatedTokenAddress(
    splToken.ASSOCIATED_TOKEN_PROGRAM_ID,
    splToken.TOKEN_PROGRAM_ID,
    new web3.PublicKey(settings.tokenAddress),
    new web3.PublicKey(address));
}

async function getNonce() {
  return (await connection.getNonce(new web3.PublicKey(settings.nonceAccount))).nonce;
}

async function signMint(destination, amount) {
  let cliQuery =
    `spl-token mint ${settings.tokenAddress} ${amount} ${await getTokenAccount(destination)} \
      --owner ${settings.multisigAccount}`;
  for (const signer of settings.multisigSigners) {
    cliQuery += ` --multisig-signer ${signer}`.replace(new RegExp(wallet.publicKey.toBase58(), 'g'), settings.keypairPath);
  }
  cliQuery += ` --blockhash ${await getNonce()} \
      --fee-payer ${settings.feePayer} \
      --nonce ${settings.nonceAccount} \
      --nonce-authority ${settings.nonceAuthority} \
      --sign-only \
      --mint-decimals ${settings.mintDecimals} \
      --output json`;

  const {
    stdout
  } = await exec(cliQuery);
  return JSON.parse(stdout).signers[0];
}

async function finalizeMintAndSend(destination, amount, signatures) {
  let cliQuery =
    `spl-token mint ${settings.tokenAddress} ${amount} ${await getTokenAccount(destination)} \
      --owner ${settings.multisigAccount}`;
  for (const signer of settings.multisigSigners) {
    cliQuery += ` --multisig-signer ${signer}`;
  }
  cliQuery += ` --blockhash ${await getNonce()} \
      --fee-payer ${settings.hotWalletKeypairPath} \
      --nonce ${settings.nonceAccount} \
      --nonce-authority ${settings.hotWalletKeypairPath} \
      --output json`;
  for (const signature of signatures) {
    cliQuery += ` --signer ${signature}`;
  }

  const {
    stdout
  } = await exec(cliQuery);
  return JSON.parse(stdout);
}

async function getBurn(burnSignature) {
  const tx = await connection.getTransaction(burnSignature, {
    encoding: 'jsonParsed'
  });
  const msg = tx.transaction.message;

  if (msg.instructions.length !== 2) {
    throw new Error('Incorrect instruction length');
  }
  if (msg.indexToProgramIds.get(msg.instructions[0].programIdIndex).toBase58() !== splToken.TOKEN_PROGRAM_ID.toBase58()) {
    throw new Error('Incorrect program for burn instruction');
  }
  if (msg.indexToProgramIds.get(msg.instructions[1].programIdIndex).toBase58() !== 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr') {
    throw new Error('Incorrect program for memo instruction');
  }


  const burnData = bs58.decode(msg.instructions[0].data);
  if (burnData[0] !== 8 || burnData.length !== 9) {
    throw new Error('Burn instruction data incorrect');
  }
  const burnAmount = burnData.slice(1).readBigInt64LE();

  const memoData = bs58.decode(msg.instructions[1].data).toString('ascii');
  if (memoData.split('|').length !== 3) {
    throw new Error('Memo data incorrect');
  }
  const burnDestination = memoData.split('|')[1];

  return {
    signature: burnSignature,
    amount: burnAmount.toString(),
    destination: burnDestination.toString()
  };
}
