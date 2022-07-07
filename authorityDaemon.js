"use strict";

const express = require("express");
const database = require("./database.js");
const dingo = require("./dingo");
const cors = require("cors");
const crypto = require("crypto");
const fs = require("fs");
const rateLimit = require("express-rate-limit");
const {
  IPBlockedError,
  default: ipfilter,
} = require("express-ip-filter-middleware");
const morgan = require("morgan");
const childProcess = require("child_process");
const AsyncLock = require("async-lock");
const util = require("util");
const https = require("https");
const tls = require("tls");
const got = require("got");
const splToken = require("./splToken");
const { createProxyMiddleware } = require("http-proxy-middleware");

const LOCALHOST = "127.0.0.1";

function getAuthorityLink(x) {
  return `https://${x.hostname}:${x.port}`;
}

const AMOUNT_THRESHOLD = BigInt(dingo.toSatoshi("100000"));
const DUST_THRESHOLD = BigInt(dingo.toSatoshi("1"));
const PAYOUT_NETWORK_FEE_PER_TX = BigInt(dingo.toSatoshi("20")); // Add this to network fee for each deposit / withdrawal.

function meetsThreshold(x) {
  return BigInt(x) >= AMOUNT_THRESHOLD;
}

function taxAmount(x) {
  return (BigInt(x) / 100n).toString();
}

function amountAfterTax(x) {
  return (BigInt(x) - BigInt(x) / 100n).toString();
}

function asyncHandler(fn) {
  return async function (req, res) {
    try {
      return await fn(req, res);
    } catch (err) {
      const stream = fs.createWriteStream("log.txt", {
        flags: "a",
      });
      stream.write(`>>>>> ERROR START [${new Date().toUTCString()}] >>>>>\n`);
      stream.write(
        err.stack +
          "\n" +
          req.path +
          "\n" +
          JSON.stringify(req.body, null, 2) +
          "\n"
      );
      stream.write("<<<<<< ERROR END <<<<<<\n");
      stream.end();
      res.status(500).json(err.stack);
    }
  };
}

function isObject(x) {
  return typeof x === "object" && x !== null && !Array.isArray(x);
}

(async function main() {
  // Load settings.
  const args = process.argv.slice(2);
  const settingsFolder = args.length >= 1 ? args[0] : "settings";
  const databaseSettings = JSON.parse(
    fs.readFileSync(`${settingsFolder}/database.json`)
  );
  const solanaSettings = JSON.parse(
    fs.readFileSync(`${settingsFolder}/solana.json`)
  );
  const dingoSettings = JSON.parse(
    fs.readFileSync(`${settingsFolder}/dingo.json`)
  );
  const publicSettings = JSON.parse(
    fs.readFileSync(`${settingsFolder}/public.json`)
  );
  const sslSettings = JSON.parse(fs.readFileSync(`${settingsFolder}/ssl.json`));

  // Initialize services.
  splToken.load(solanaSettings);
  database.load(databaseSettings.databasePath);
  async function post(link, data) {
    const r = await got.post(link, {
      body: JSON.stringify(data),
      timeout: {
        request: 10000,
      },
      headers: {
        "Content-Type": "application/json",
      },
      retry: 0,
    });
    return JSON.parse(r.body);
  }

  // DB write lock.
  const lock = new AsyncLock();
  const acquire = function (fn) {
    return lock.acquire("lock", fn);
  };

  // Stats lock.
  const statsLock = new AsyncLock();
  const acquireStats = function (fn) {
    return statsLock.acquire("statsLock", fn);
  };
  let stats = null;

  // Utility functions.
  const createIpFilter = (x) =>
    ipfilter({
      mode: "whitelist",
      allow: x,
    });
  const createRateLimit = (windowS, count) =>
    rateLimit({
      windowMs: windowS * 1000,
      max: count,
    });
  const createTimedAndSignedMessage = async (x) => {
    if (!isObject(x)) {
      throw new Error(`Cannot sign non-object ${JSON.stringify(x)}`);
    }
    const blockchainInfo = await dingo.getBlockchainInfo();
    x.valDingoHeight = blockchainInfo.blocks - dingoSettings.syncDelayThreshold;
    x.valDingoHash = await dingo.getBlockHash(
      blockchainInfo.blocks - dingoSettings.syncDelayThreshold
    );
    return splToken.createSignedMessage(x);
  };
  const validateTimedAndSignedMessage = async (
    x,
    walletAddress,
    discard = true
  ) => {
    if (!isObject(x.data)) {
      throw new Error("Data is non-object");
    }
    const blockchainInfo = await dingo.getBlockchainInfo();
    if (
      x.data.valDingoHeight <
      blockchainInfo.blocks - 2 * dingoSettings.syncDelayThreshold
    ) {
      throw new Error("Message expired");
    }
    if (
      x.data.valDingoHash !== (await dingo.getBlockHash(x.data.valDingoHeight))
    ) {
      throw new Error("Verification failed: incorrect chain");
    }
    return splToken.validateSignedMessage(x, walletAddress, discard);
  };
  const validateTimedAndSignedMessageOne = async (
    x,
    walletAddresses,
    discard = true
  ) => {
    if (!isObject(x.data)) {
      throw new Error(`Data is non-object: ${JSON.stringify(x)}`);
    }
    const blockchainInfo = await dingo.getBlockchainInfo();
    if (
      x.data.valDingoHeight <
      blockchainInfo.blocks - 2 * dingoSettings.syncDelayThreshold
    ) {
      throw new Error("Message expired");
    }
    if (
      x.data.valDingoHash !== (await dingo.getBlockHash(x.data.valDingoHeight))
    ) {
      throw new Error("Verification failed: incorrect chain");
    }
    return splToken.validateSignedMessageOne(x, walletAddresses, discard);
  };

  // Compute version on launch.
  const version = {
    repository: childProcess
      .execSync("git config --get remote.origin.url")
      .toString()
      .trim(),
    hash: childProcess.execSync("git rev-parse HEAD").toString().trim(),
    timestamp:
      parseInt(
        childProcess
          .execSync('git --no-pager log --pretty=format:"%at" -n1')
          .toString()
          .trim()
      ) * 1000,
    clean: childProcess.execSync("git diff --stat").toString().trim() === "",
    dingoVersion: await dingo.getClientVersion(),
  };

  const app = express();
  app.use(cors());
  app.options('*', cors());  // enable pre-flight
  app.use(express.json());

  app.post(
    "/ping",
    createRateLimit(10, 10),
    asyncHandler(async (req, res) => {
      res.send(
        await createTimedAndSignedMessage({
          timestamp: Date.now(),
        })
      );
    })
  );

  app.post(
    "/generateDepositAddress",
    createRateLimit(20, 1),
    asyncHandler(async (req, res) => {
      const data = req.body;
      const mintAddress = data.mintAddress;
      if (!splToken.isAddress(mintAddress)) {
        throw new Error("mintAddress missing or invalid");
      }
      if (!(await splToken.hasTokenAccount)) {
        throw new Error("Token account not found for wallet");
      }

      res.send(
        await createTimedAndSignedMessage({
          mintAddress: data.mintAddress,
          depositAddress: await dingo.getNewAddress(),
        })
      );
    })
  );

  app.post(
    "/registerMintDepositAddress",
    createRateLimit(20, 1),
    asyncHandler(async (req, res) => {
      const data = req.body;
      if (
        data.generateDepositAddressResponses.length !==
        publicSettings.authorityNodes.length
      ) {
        throw new Error("Incorrect authority count");
      }
      const generateDepositAddressResponses = await Promise.all(
        data.generateDepositAddressResponses.map((x, i) =>
          validateTimedAndSignedMessage(
            x,
            publicSettings.authorityNodes[i].walletAddress
          )
        )
      );
      if (
        !generateDepositAddressResponses.every(
          (x) =>
            x.mintAddress === generateDepositAddressResponses[0].mintAddress
        )
      ) {
        throw new Error("Consensus failure on mint address");
      }
      const mintAddress = generateDepositAddressResponses[0].mintAddress;
      if (!splToken.isAddress(mintAddress)) {
        throw new Error("mintAddress missing or invalid");
      }

      await acquire(async () => {
        const depositAddresses = generateDepositAddressResponses.map(
          (x) => x.depositAddress
        );
        if (await database.hasUsedDepositAddresses(depositAddresses)) {
          throw new Error(
            "At least one deposit address has been previously registered"
          );
        }

        // Register as previously used.
        await database.registerUsedDepositAddresses(depositAddresses);

        // Compute multisigDepositAddress.
        const { address: multisigDepositAddress, redeemScript } =
          await dingo.createMultisig(
            publicSettings.authorityThreshold,
            depositAddresses
          );
        try {
          await dingo.importAddress(redeemScript);
        } catch (err) {}

        // Register mintDepositAddress.
        await database.registerMintDepositAddress(
          mintAddress,
          multisigDepositAddress,
          redeemScript
        );

        res.send(
          await createTimedAndSignedMessage({
            depositAddress: multisigDepositAddress,
          })
        );
      });
    })
  );

  app.post(
    "/queryMintBalance",
    createRateLimit(10, 10),
    asyncHandler(async (req, res) => {
      const data = req.body;
      const mintAddress = data.mintAddress;
      if (!splToken.isAddress(mintAddress)) {
        throw new Error("mintAddress missing or invalid");
      }

      // Retrieve deposit address.
      const depositAddress = await database.getMintDepositAddress(mintAddress);

      if (depositAddress === null) {
        throw new Error("Mint address not registered");
      }

      // Retrieve deposited amount.
      const depositedAmount = dingo.toSatoshi(
        (
          await dingo.getReceivedAmountByAddress(
            dingoSettings.depositConfirmations,
            depositAddress.depositAddress
          )
        ).toString()
      );
      const unconfirmedAmount =
        dingo.toSatoshi(
          (
            await dingo.getReceivedAmountByAddress(
              0,
              depositAddress.depositAddress
            )
          ).toString()
        ) - depositedAmount;
      const approvedAmount = depositAddress.approvedAmount;

      res.send(
        await createTimedAndSignedMessage({
          mintAddress: mintAddress,
          depositAddress: depositAddress.depositAddress,
          depositedAmount: amountAfterTax(depositedAmount).toString(),
          unconfirmedAmount: amountAfterTax(unconfirmedAmount).toString(),
          approvedAmount: approvedAmount,
        })
      );
    })
  );

  app.post(
    "/computePendingMint",
    createRateLimit(5, 1),
    asyncHandler(async (req, res) => {
      const deposited = await dingo.listReceivedByAddress(
        dingoSettings.depositConfirmations
      );
      const nonEmptyMintDepositAddresses =
        await database.getMintDepositAddresses(Object.keys(deposited));
      const pendingMint = [];
      for (const a of nonEmptyMintDepositAddresses) {
        const depositedAmount = dingo.toSatoshi(
          deposited[a.depositAddress].amount.toString()
        );
        const mintableAmount = amountAfterTax(depositedAmount);
        const approvedAmount = a.approvedAmount;
        if (meetsThreshold(mintableAmount - approvedAmount)) {
          pendingMint.push({
            mintAddress: a.mintAddress,
            depositAddress: a.depositAddress,
            approvedAmount: approvedAmount.toString(),
            mintAmount: (mintableAmount - approvedAmount).toString(),
          });
        }
      }

      res.send(await createTimedAndSignedMessage({ pendingMint: pendingMint }));
    })
  );

  const makeApproveMintHandler = (test) => {
    return async (req, res) => {
      // Coordinator only.
      const data = await validateTimedAndSignedMessage(
        req.body,
        publicSettings.authorityNodes[publicSettings.payoutCoordinator]
          .walletAddress
      );
      const mint = data.mint;
      await acquire(async () => {
        const deposited = await dingo.getReceivedAmountByAddress(
          dingoSettings.depositConfirmations,
          mint.depositAddress
        );
        const depositAddress = await database.getMintDepositAddress(
          mint.mintAddress
        );
        if (depositAddress.depositAddress !== mint.depositAddress) {
          throw new Error("Deposit address details incorrect");
        }

        const depositedAmount = dingo.toSatoshi(deposited);
        const mintableAmount = amountAfterTax(depositedAmount);
        const approvedAmount = depositAddress.approvedAmount;
        const mintAmount = BigInt(mint.mintAmount);

        if (mintableAmount - approvedAmount < mintAmount) {
          throw new Error("Insufficient mint balance");
        }

        if (!test) {
          const signature = await splToken.signMint(
            mint.mintAddress,
            dingo.fromSatoshi(mintAmount.toString())
          );
          depositAddress.approvedAmount = (
            BigInt(depositAddress.approvedAmount) + mintAmount
          ).toString();
          await database.updateMintDepositAddresses([depositAddress]);
          res.send(
            await createTimedAndSignedMessage({
              signature: signature,
            })
          );
        } else {
          // Test - DO NOT RETURN SIGNATURE.
          await splToken.signMint(
            mint.mintAddress,
            dingo.fromSatoshi(mintAmount.toString())
          );
          res.send(
            await createTimedAndSignedMessage({
              signature: null,
            })
          );
        }
      });
    };
  };
  app.post(
    "/approveMint",
    createRateLimit(1, 1),
    asyncHandler(makeApproveMintHandler(false))
  );
  app.post(
    "/approveMintTest",
    createRateLimit(1, 1),
    asyncHandler(makeApproveMintHandler(true))
  );

  app.post(
    "/queryBurnHistory",
    createRateLimit(10, 10),
    asyncHandler(async (req, res) => {
      const data = req.body;
      const burnHistory = data.burnHistory;

      for (const burn of burnHistory) {
        const w = await database.getWithdrawal(burn.burnSignature);
        burn.status =
          w === null
            ? null
            : BigInt(w.approvedTax) === BigInt(0)
            ? "SUBMITTED"
            : "APPROVED";
      }

      res.send(
        await createTimedAndSignedMessage({
          burnHistory: burnHistory,
        })
      );
    })
  );

  app.post(
    "/submitWithdrawal",
    createRateLimit(1, 5),
    asyncHandler(async (req, res) => {
      const data = req.body;
      const burn = data.burn;

      // Sanity checks.
      if (!(await dingo.verifyAddress(burn.burnDestination))) {
        throw new Error("Withdrawal address is not a valid Dingo address");
      }
      if (!meetsThreshold(BigInt(burn.burnAmount))) {
        throw new Error("Withdrawal amount does not meet threshold");
      }

      await acquire(async () => {
        // Check duplicate submission.
        if ((await database.getWithdrawal(burn.burnSignature)) !== null) {
          throw new Error("Withdrawal already submitted");
        }

        // Verify burn status on-chain.
        const chainBurn = await splToken.getBurn(burn.burnSignature);
        if (BigInt(burn.burnAmount) !== BigInt(chainBurn.amount)) {
          throw new Error("Burn amount does not match tranasction");
        }
        if (burn.burnDestination !== chainBurn.destination) {
          throw new Error("Burn destination does not match tranasction");
        }

        await database.registerWithdrawal(burn);

        res.send(await createTimedAndSignedMessage({}));
      });
    })
  );

  // Compute pending payouts:
  // 1) Tax payouts from deposits (10 + 1%).
  // 2) Withdrawal payouts.
  // 3) Tax payouts from withdrawals (10 + 1%).
  const computePendingPayouts = async (processDeposits, processWithdrawals) => {
    const depositTaxPayouts = []; // Track which deposit taxes are being paid.
    const withdrawalPayouts = []; // Track which withdrawals are being paid.
    const withdrawalTaxPayouts = []; // Track which withdrawal taxes are being paid.

    // Compute tax from deposits.
    if (processDeposits) {
      const deposited = await dingo.listReceivedByAddress(
        dingoSettings.depositConfirmations
      );
      const nonEmptyMintDepositAddresses =
        await database.getMintDepositAddresses(Object.keys(deposited));
      for (const a of nonEmptyMintDepositAddresses) {
        const depositedAmount = dingo.toSatoshi(
          deposited[a.depositAddress].amount.toString()
        );
        const approvedTax = BigInt(a.approvedTax);
        const approvableTax = BigInt(taxAmount(depositedAmount));
        if (approvableTax > approvedTax) {
          const payoutAmount = approvableTax - approvedTax;
          depositTaxPayouts.push({
            depositAddress: a.depositAddress,
            amount: payoutAmount.toString(),
          });
        } else if (approvableTax < approvedTax) {
          throw new Error("Deposit approved tax exceeds approvable");
        }
      }
    }

    // Query unapproved withdrawals.
    if (processWithdrawals) {
      const unapprovedWithdrawals = await database.getUnapprovedWithdrawals();
      // Compute unapproved withdrawal payouts and tax from withdrawals.
      for (const w of unapprovedWithdrawals) {
        withdrawalPayouts.push({
          burnSignature: w.burnSignature,
          burnDestination: w.burnDestination,
          amount: amountAfterTax(w.burnAmount).toString(),
        });
        withdrawalTaxPayouts.push({
          burnSignature: w.burnSignature,
          burnDestination: w.burnDestination,
          amount: taxAmount(w.burnAmount).toString(),
        });
      }
    }

    return {
      depositTaxPayouts: depositTaxPayouts,
      withdrawalPayouts: withdrawalPayouts,
      withdrawalTaxPayouts: withdrawalTaxPayouts,
    };
  };
  app.post(
    "/computePendingPayouts",
    createRateLimit(5, 1),
    asyncHandler(async (req, res) => {
      const data = await validateTimedAndSignedMessageOne(
        req.body,
        publicSettings.authorityNodes.map((x) => x.walletAddress)
      );
      res.send(
        await createTimedAndSignedMessage(
          await computePendingPayouts(
            data.processDeposits,
            data.processWithdrawals
          )
        )
      );
    })
  );

  const validatePayouts = async (
    depositTaxPayouts,
    withdrawalPayouts,
    withdrawalTaxPayouts
  ) => {
    const totalTax =
      depositTaxPayouts.reduce((a, b) => a + BigInt(b.amount), 0n) +
      withdrawalTaxPayouts.reduce((a, b) => a + BigInt(b.amount), 0n);
    const networkFee =
      BigInt(depositTaxPayouts.length + withdrawalPayouts.length) *
      PAYOUT_NETWORK_FEE_PER_TX;
    if (totalTax < networkFee) {
      throw new Error(
        `Insufficient tax to cover network fees of ${dingo.fromSatoshi(
          networkFee
        )}`
      );
    }

    // Check if requested tax from deposits does not exceed taxable.
    const deposited = await dingo.listReceivedByAddress(
      dingoSettings.depositConfirmations
    );
    const depositAddresses = {};
    (await database.getMintDepositAddresses(Object.keys(deposited))).forEach(
      (x) => (depositAddresses[x.depositAddress] = x)
    );

    for (const p of depositTaxPayouts) {
      if (!(p.depositAddress in deposited)) {
        throw new Error("Dingo address has zero balance");
      }
      if (!(p.depositAddress in depositAddresses)) {
        throw new Error("Dingo address not registered");
      }
      const depositedAmount = dingo.toSatoshi(
        deposited[p.depositAddress].amount.toString()
      );
      const approvedTax = BigInt(
        depositAddresses[p.depositAddress].approvedTax
      );
      const approvableTax = BigInt(taxAmount(depositedAmount));
      if (BigInt(p.amount) + approvedTax > approvableTax) {
        throw new Error(
          "Requested tax amount more than remaining approvable tax"
        );
      }
    }

    // Query unapproved withdrawals.
    if (withdrawalPayouts.length !== withdrawalTaxPayouts.length) {
      throw new Error(
        "Withdrawal and withdrawal tax payouts mismatch in count"
      );
    }
    // Compute unapproved withdrawal payouts and tax from withdrawals.
    for (const i in withdrawalPayouts) {
      if (
        withdrawalPayouts[i].burnSignature !==
        withdrawalTaxPayouts[i].burnSignature
      ) {
        throw new Error(
          "Mismatch in withdrawal and withdrawal tax payout signatures"
        );
      }
      const withdrawal = await database.getWithdrawal(
        withdrawalPayouts[i].burnSignature
      );
      if (withdrawal === null) {
        throw new Error("Withdrawal not registered");
      }
      if (
        BigInt(withdrawal.approvedAmount) !== BigInt("0") ||
        BigInt(withdrawal.approvedTax) !== BigInt("0")
      ) {
        throw new Error("Withdrawal already approved");
      }
      if (withdrawalPayouts[i].burnDestination !== withdrawal.burnDestination) {
        throw new Error("Withdrawal destination incorrect");
      }
      if (
        withdrawalTaxPayouts[i].burnDestination !== withdrawal.burnDestination
      ) {
        throw new Error("Withdrawal tax destination incorrect");
      }
      if (
        BigInt(withdrawalPayouts[i].amount) !==
        BigInt(amountAfterTax(withdrawal.burnAmount))
      ) {
        throw new Error("Withdrawal amount incorrect");
      }
      if (
        BigInt(withdrawalTaxPayouts[i].amount) !==
        BigInt(taxAmount(withdrawal.burnAmount))
      ) {
        throw new Error("Withdrawal tax amount incorrect");
      }
    }
  };

  // Computes UTXOs among deposits and change.
  const computeUnspent = async () => {
    const changeUtxos = await dingo.listUnspent(
      dingoSettings.changeConfirmations,
      [dingoSettings.changeAddress]
    );
    const deposited = await dingo.listReceivedByAddress(
      dingoSettings.depositConfirmations
    );
    const nonEmptyMintDepositAddresses = await database.getMintDepositAddresses(
      Object.keys(deposited)
    );
    const depositUtxos = await dingo.listUnspent(
      dingoSettings.depositConfirmations,
      nonEmptyMintDepositAddresses.map((x) => x.depositAddress)
    );
    return changeUtxos.concat(depositUtxos);
  };
  app.post(
    "/computeUnspent",
    createRateLimit(5, 1),
    asyncHandler(async (req, res) => {
      await validateTimedAndSignedMessageOne(
        req.body,
        publicSettings.authorityNodes.map((x) => x.walletAddress)
      );
      res.send(
        await createTimedAndSignedMessage({
          unspent: await computeUnspent(),
        })
      );
    })
  );

  // Checks if UTXOs exist among deposits and change.
  const validateUnspent = async (unspent) => {
    const _unspent = await computeUnspent();

    const hash = (x) =>
      `${x.txid}|${x.vout}|${x.address}|${x.scriptPubKey}|${x.amount}`;

    const _unspent_set = new Set();
    for (const x of _unspent) {
      _unspent_set.add(hash(x));
    }

    for (const x of unspent) {
      if (!_unspent_set.has(hash(x))) {
        throw new Error("Non-existent UTXO");
      }
    }
  };

  // Compute vouts for raw transaction from payouts and UTXOs.
  const computeVouts = async (
    depositTaxPayouts,
    withdrawalPayouts,
    withdrawalTaxPayouts,
    unspent
  ) => {
    // Process withdrawal payouts.
    const vouts = {};
    for (const p of withdrawalPayouts) {
      if (p.burnDestination in vouts) {
        vouts[p.burnDestination] += BigInt(p.amount);
      } else {
        vouts[p.burnDestination] = BigInt(p.amount);
      }
    }

    // Compute tax payouts.
    const totalTax =
      depositTaxPayouts.reduce((a, b) => a + BigInt(b.amount), 0n) +
      withdrawalTaxPayouts.reduce((a, b) => a + BigInt(b.amount), 0n);
    const networkFee =
      BigInt(depositTaxPayouts.length + withdrawalPayouts.length) *
      PAYOUT_NETWORK_FEE_PER_TX;
    if (totalTax < networkFee) {
      throw new Error(`Insufficient tax for network fee of ${networkFee}`);
    }
    const taxPayoutPerPayee =
      (totalTax - networkFee) / BigInt(dingoSettings.taxPayoutAddresses.length);
    for (const a of dingoSettings.taxPayoutAddresses) {
      if (a in vouts) {
        vouts[a] += taxPayoutPerPayee;
      } else {
        vouts[a] = taxPayoutPerPayee;
      }
    }

    // Compute total payout.
    const totalPayout = Object.values(vouts).reduce((a, b) => a + b, 0n);

    // Compute change.
    const totalUnspent = unspent.reduce(
      (a, b) => a + BigInt(dingo.toSatoshi(b.amount.toString())),
      BigInt(0)
    );
    const change = totalUnspent - totalPayout - networkFee; // Rounding errors from taxPayout / N is absorbed
    // into change here.
    if (change < 0) {
      throw new Error("Insufficient funds");
    }
    if (change > 0) {
      if (dingoSettings.changeAddress in vouts) {
        vouts[dingoSettings.changeAddress] += change;
      } else {
        vouts[dingoSettings.changeAddress] = change;
      }
    }

    // Convert to string.
    const voutsFinal = {};
    for (const address of Object.keys(vouts)) {
      if (vouts[address] >= DUST_THRESHOLD) {
        voutsFinal[address] = dingo.fromSatoshi(vouts[address].toString());
      }
    }

    return voutsFinal;
  };

  const applyPayouts = async (
    depositTaxPayouts,
    withdrawalPayouts,
    withdrawalTaxPayouts
  ) => {
    const depositAddresses = {};
    (
      await database.getMintDepositAddresses(
        depositTaxPayouts.map((x) => x.depositAddress)
      )
    ).forEach((x) => (depositAddresses[x.depositAddress] = x));
    for (const p of depositTaxPayouts) {
      const previousTax = BigInt(
        depositAddresses[p.depositAddress].approvedTax
      );
      const tax = BigInt(p.amount);
      depositAddresses[p.depositAddress].approvedTax = (
        previousTax + tax
      ).toString();
    }
    await database.updateMintDepositAddresses(Object.values(depositAddresses));

    const withdrawals = [];
    for (const i in withdrawalPayouts) {
      const withdrawal = await database.getWithdrawal(
        withdrawalPayouts[i].burnSignature
      );
      const previousApprovedAmount = BigInt(withdrawal.approvedAmount);
      const previousApprovedTax = BigInt(withdrawal.approvedTax);
      const amount = BigInt(withdrawalPayouts[i].amount);
      const tax = BigInt(withdrawalTaxPayouts[i].amount);
      withdrawal.approvedAmount = (previousApprovedAmount + amount).toString();
      withdrawal.approvedTax = (previousApprovedTax + tax).toString();
      withdrawals.push(withdrawal);
    }
    await database.updateWithdrawals(withdrawals);
  };

  const makeApprovePayoutsHandler = (test) => {
    return async (req, res) => {
      await acquire(async () => {
        // Extract info.
        let {
          depositTaxPayouts,
          withdrawalPayouts,
          withdrawalTaxPayouts,
          unspent,
          approvalChain,
        } = await validateTimedAndSignedMessage(
          req.body,
          publicSettings.authorityNodes[publicSettings.payoutCoordinator]
            .walletAddress
        );

        // Validate unspent.
        await validateUnspent(unspent);

        // Validate payouts.
        await validatePayouts(
          depositTaxPayouts,
          withdrawalPayouts,
          withdrawalTaxPayouts
        );

        // Compute vouts.
        const vouts = await computeVouts(
          depositTaxPayouts,
          withdrawalPayouts,
          withdrawalTaxPayouts,
          unspent
        );

        if (approvalChain === null) {
          approvalChain = await dingo.createRawTransaction(unspent, vouts);
        }

        // Validate utxos and payouts against transaction and sign.
        await dingo.verifyRawTransaction(unspent, vouts, approvalChain);

        if (!test) {
          const approvalChainNext = await dingo.signRawTransaction(
            approvalChain
          );
          await applyPayouts(
            depositTaxPayouts,
            withdrawalPayouts,
            withdrawalTaxPayouts
          );
          res.send(
            await createTimedAndSignedMessage({
              approvalChain: approvalChainNext,
            })
          );
        } else {
          await dingo.signRawTransaction(approvalChain);
          res.send(
            await createTimedAndSignedMessage({
              approvalChain: approvalChain,
            })
          );
        }
      });
    };
  };
  app.post("/approvePayouts", asyncHandler(makeApprovePayoutsHandler(false)));
  app.post(
    "/approvePayoutsTest",
    asyncHandler(makeApprovePayoutsHandler(true))
  );

  app.post(
    "/stats",
    createRateLimit(5, 1),
    asyncHandler(async (req, res) => {
      acquireStats(async () => {
        if (
          stats === null ||
          new Date().getTime() - stats.time >= 1000 * 60 * 10
        ) {
          const newStats = {
            version: version,
            time: new Date().getTime(),
            publicSettings: publicSettings,
            dingoSettings: dingoSettings,
            solanaSettings: {
              cluster: solanaSettings.cluster,
              tokenAddress: solanaSettings.tokenAddress,
              multisigAccount: solanaSettings.multisigAccount,
              multisigSigners: solanaSettings.multisigSigners,
              nonceAccount: solanaSettings.nonceAccount,
              nonceAuthority: solanaSettings.nonceAuthority,
              feePayer: solanaSettings.feePayer,
              mintDecimals: solanaSettings.mintDecimals,
            },
            confirmedDeposits: {},
            unconfirmedDeposits: {},
            withdrawals: {},
            confirmedUtxos: {},
            unconfirmedUtxos: {},
          };

          newStats.publicSettings.walletAddress = childProcess
            .execSync(`solana-keygen pubkey ${solanaSettings.keypairPath}`)
            .toString()
            .trim();

          // Process deposits.
          const depositAddresses = await database.getMintDepositAddresses();
          const computeDeposits = async (confirmations, output) => {
            output.count = depositAddresses.length;
            const depositedAmounts = await dingo.getReceivedAmountByAddresses(
              confirmations,
              depositAddresses.map((x) => x.depositAddress)
            );
            const totalDepositedAmount = Object.values(depositedAmounts)
              .reduce((a, b) => a + BigInt(dingo.toSatoshi(b.toString())), 0n)
              .toString();
            const totalApprovableAmount = Object.values(depositedAmounts)
              .reduce((a, b) => a + BigInt(amountAfterTax(dingo.toSatoshi(b.toString()))), 0n)
              .toString();
            const totalApprovedAmount = depositAddresses
              .reduce((a, b) => a + BigInt(b.approvedAmount), 0n)
              .toString();
            const remainingApprovableAmount = (
              BigInt(totalApprovableAmount) - BigInt(totalApprovedAmount)
            ).toString();
            const totalApprovableTax = Object.values(depositedAmounts)
              .reduce(
                (a, b) => a + BigInt(taxAmount(dingo.toSatoshi(b.toString()))),
                0n
              )
              .toString();
            const totalApprovedTax = depositAddresses
              .reduce((a, b) => a + BigInt(b.approvedTax), 0n)
              .toString();
            const remainingApprovableTax = (
              BigInt(totalApprovableTax) - BigInt(totalApprovedTax)
            ).toString();

            output.totalDepositedAmount = totalDepositedAmount;
            output.totalApprovableAmount = totalApprovableAmount;
            output.totalApprovedAmount = totalApprovedAmount;
            output.remainingApprovableAmount = remainingApprovableAmount;
            output.totalApprovableTax = totalApprovableTax;
            output.totalApprovedTax = totalApprovedTax;
            output.remainingApprovableTax = remainingApprovableTax;
          };
          await computeDeposits(
            dingoSettings.depositConfirmations,
            newStats.confirmedDeposits
          );
          await computeDeposits(0, newStats.unconfirmedDeposits);

          // Process withdrawals.
          const withdrawals = await database.getWithdrawals();
          newStats.withdrawals.count = withdrawals.length;
          newStats.withdrawals.totalBurnedAmount = withdrawals
            .reduce((a, b) => a + BigInt(b.burnAmount), 0n)
            .toString();
          newStats.withdrawals.totalApprovableAmount = withdrawals
            .reduce((a, b) => a + BigInt(amountAfterTax(b.burnAmount)), 0n)
            .toString();
          newStats.withdrawals.totalApprovedAmount = withdrawals
            .reduce((a, b) => a + BigInt(b.approvedAmount), 0n)
            .toString();
          newStats.withdrawals.totalApprovableTax = withdrawals
            .reduce((a, b) => a + BigInt(taxAmount(b.burnAmount)), 0n)
            .toString();
          newStats.withdrawals.totalApprovedTax = withdrawals
            .reduce((a, b) => a + BigInt(b.approvedTax), 0n)
            .toString();
          newStats.withdrawals.remainingApprovableAmount = (
            BigInt(newStats.withdrawals.totalApprovableAmount) -
            BigInt(newStats.withdrawals.totalApprovedAmount)
          ).toString();
          newStats.withdrawals.remainingApprovableTax = (
            BigInt(newStats.withdrawals.totalApprovableTax) -
            BigInt(newStats.withdrawals.totalApprovedTax)
          ).toString();

          // Process UTXOs.
          const computeUtxos = async (
            changeConfirmations,
            depositConfirmations,
            output
          ) => {
            const changeUtxos = await dingo.listUnspent(changeConfirmations, [
              dingoSettings.changeAddress,
            ]);
            const depositUtxos = await dingo.listUnspent(
              depositConfirmations,
              depositAddresses.map((x) => x.depositAddress)
            );
            output.totalChangeBalance = changeUtxos
              .reduce(
                (a, b) => a + BigInt(dingo.toSatoshi(b.amount.toString())),
                0n
              )
              .toString();
            output.totalDepositsBalance = depositUtxos
              .reduce(
                (a, b) => a + BigInt(dingo.toSatoshi(b.amount.toString())),
                0n
              )
              .toString();
          };
          await computeUtxos(
            dingoSettings.changeConfirmations,
            dingoSettings.depositConfirmations,
            newStats.confirmedUtxos
          );
          await computeUtxos(0, 0, newStats.unconfirmedUtxos);
          stats = newStats;
        }
        res.send(await createTimedAndSignedMessage(stats));
      });
    })
  );

  app.post("/dumpDatabase", async (req, res) => {
    const data = req.body;
    await validateTimedAndSignedMessageOne(
      data,
      publicSettings.authorityNodes.map((x) => x.walletAddress)
    );
    res.send({
      sql: await database.dump(databaseSettings.databasePath),
    });
  });

  let server = null;
  app.post("/dingoDoesAHarakiri", async (req, res) => {
    const data = await validateTimedAndSignedMessageOne(
      req.body,
      publicSettings.authorityNodes.map((x) => x.walletAddress)
    );
    console.log(
      `TERMINATING! Suicide signal received.\nMessage: ${data.message}`
    );
    res.send({});
    server.close();
  });

  app.post(
    "/log",
    createRateLimit(5, 1),
    asyncHandler(async (req, res) => {
      const data = req.body;
      await validateTimedAndSignedMessageOne(
        data,
        publicSettings.authorityNodes.map((x) => x.walletAddress)
      );
      res.send({
        log: await util.promisify(fs.readFile)("log.txt", "utf8"),
      });
    })
  );

  app.use((err, req, res, _next) => {
    if (err instanceof IPBlockedError) {
      res
        .status(401)
        .send(`Access forbidden from ${req.header("x-forwarded-for")}`);
    } else {
      res.status(err.status || 500).send("Internal server error");
    }
  });

  server = https.createServer({
    key: fs.readFileSync(sslSettings.keyPath),
    cert: fs.readFileSync(sslSettings.certPath),
    SNICallback: (domain, cb) => {
      cb(null, tls.createSecureContext({
        key: fs.readFileSync(sslSettings.keyPath),
        cert: fs.readFileSync(sslSettings.certPath),
      }));
    }
  }, app).listen(publicSettings.port, () => {
    console.log(`Started on port ${publicSettings.port}`);
  });
})();
