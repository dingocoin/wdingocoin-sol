const Table = require("tty-table");
const chalk = require("chalk");
const database = require("./database.js");
const dingo = require("./dingo");
const fs = require("fs");
const got = require("got");
const splToken = require("./splToken");

function getAuthorityLink(x) {
  return `https://${x.hostname}:${x.port}`;
}

function getStyledAuthorityLink(x) {
  return chalk.blue.bold(`[${getAuthorityLink(x)}]`);
}

function getStyledError(code, message) {
  if (code !== null && code !== undefined) {
    if (message !== null && message !== undefined) {
      return chalk.red.bold(`Error ${code}: ${message}`);
    } else {
      return chalk.red.bold(`Error ${code}`);
    }
  } else {
    if (message !== null && message !== undefined) {
      return chalk.red.bold(`Error: ${message}`);
    } else {
      return chalk.red.bold(`Error`);
    }
  }
}

// wtf js
function isObject(x) {
  return typeof x === "object" && x !== null && !Array.isArray(x);
}

function parseBool(s) {
  if (s === "true") {
    return true;
  } else if (s === "false") {
    return false;
  } else {
    throw new Error(`Unable to parse bool string: ${s}`);
  }
}

(function () {
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

  // Utility functions.
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

  const repl = require("repl").start({
    prompt: chalk.bold("wDingocoin > "),
    eval: eval,
    writer: (x) => x,
    ignoreUndefined: true,
  });
  require("repl.history")(repl, ".cli_history");

  const commandCallbacks = {
    help: help,

    createMintDepositAddress: createMintDepositAddress,
    queryMintBalance: queryMintBalance,
    queryBurnHistory: queryBurnHistory,

    executeMint: executeMint,
    executeMintTest: executeMintTest,

    executePayouts: executePayouts,
    executePayoutsTest: executePayoutsTest,

    consensus: consensus,
    log: log,
    syncDatabase: syncDatabase,
    dingoDoesAHarakiri: dingoDoesAHarakiri,
  };

  async function eval(cmd, context, filename, callback) {
    const tokens = cmd
      .trim()
      .split(" ")
      .filter((x) => x !== "");
    if (cmd.trim().length === 0) {
      callback(null);
    } else if (!(tokens[0] in commandCallbacks)) {
      callback(`Unknown command: ${tokens[0]}`);
    } else {
      callback(null, await commandCallbacks[tokens[0]](...tokens.slice(1)));
    }
  }

  function help() {
    console.log(
      `
Available commands:

  ${chalk.bold("help")}: Prints this command.

  ${chalk.bold(
    "createMintDepositAddress <walletAddress>"
  )}: Creates a deposit address for <wallet address>.
  ${chalk.bold(
    "queryMintBalance <walletAddress>"
  )}: Queries the amount of deposited Dingocoins and minted wDingocoins for <wallet address>.
  ${chalk.bold(
    "queryBurnHistory <walletAddress>"
  )}: Queries the amount of burned wDingocoins and withdrawn Dingocoins for <wallet address>.

  ${chalk.bold("executeMint")}: ${chalk.bold.red(
        "[COORDINATOR ONLY]"
      )} Executes mint.
  ${chalk.bold("executeMintTest")}: ${chalk.bold.red(
        "[COORDINATOR ONLY]"
      )} Tests the execution of mint.

  ${chalk.bold(
    "executePayouts <processDeposits> <processWithdrawals>"
  )}: ${chalk.bold.red("[COORDINATOR ONLY]")} Executes payouts.
  ${chalk.bold(
    "executePayoutsTest <processDeposits> <processWithdrawals>"
  )}: ${chalk.bold.red("[COORDINATOR ONLY]")} Tests the execution of payouts.

  ${chalk.bold(
    "consensus"
  )}: Retrieves the state of all nodes and checks the consensus of state.
  ${chalk.bold("log <nodeIndex>")}: ${chalk.bold.red(
        "[AUTHORITY ONLY]"
      )} Retrieves the log from node <nodeIndex>.
  ${chalk.bold("syncDatabase <nodeIndex>")}: ${chalk.bold.red(
        "[AUTHORITY ONLY]"
      )} Replaces the local database with that downloaded from node <nodeIndex>.
  ${chalk.bold("dingoDoesAHarakiri <nodeIndex> <message>")}: ${chalk.bold.red(
        "[AUTHORITY ONLY]"
      )} Sends a suicide signal to node <nodeIndex> with message <message>.
  ${chalk.bold("dingoDoesAHarakiri")}: ${chalk.bold.red(
        "[AUTHORITY ONLY]"
      )} Sends a suicide signal to all nodes.
`
    );
  }

  async function createMintDepositAddress(mintAddress) {
    const results1 = [];
    console.log("Requesting new individual deposit addresses from nodes...");
    for (const x of publicSettings.authorityNodes) {
      process.stdout.write(
        `  ${getStyledAuthorityLink(x)} ${chalk.bold("->")} `
      );
      try {
        const result = await post(
          `${getAuthorityLink(x)}/generateDepositAddress`,
          {
            mintAddress: mintAddress,
          }
        );
        results1.push(result);
        console.log(`pubKey: ${result.data.depositAddress}`);
      } catch (error) {
        results1.push(undefined);
        if (error.response) {
          console.log(
            getStyledError(error.response.statusCode, error.response.body)
          );
        } else {
          console.log(getStyledError(null, error.message));
        }
      }
    }
    if (results1.some((x) => x === undefined)) {
      console.log(
        getStyledError(
          null,
          "Failed to collect new individual deposit addresses from all nodes. Aborting..."
        )
      );
      return;
    }

    const results2 = [];
    console.log("Registering new multisig deposit address with nodes...");
    for (const x of publicSettings.authorityNodes) {
      process.stdout.write(
        `  ${getStyledAuthorityLink(x)} ${chalk.bold("->")} `
      );
      try {
        const result = splToken.validateSignedMessage(
          await post(`${getAuthorityLink(x)}/registerMintDepositAddress`, {
            mintAddress: mintAddress,
            generateDepositAddressResponses: results1,
          }),
          x.walletAddress
        );
        results2.push(result);
        console.log(`multisigDepositAddress: ${result.depositAddress}`);
      } catch (error) {
        results2.push(undefined);
        if (error.response) {
          console.log(
            getStyledError(error.response.statusCode, error.response.body)
          );
        } else {
          console.log(getStyledError(null, error.message));
        }
      }
    }
    if (results2.some((x) => x === undefined)) {
      return;
    }

    if (
      !results2.every((x) => x.depositAddress === results2[0].depositAddress)
    ) {
      return console.log(
        getStyledError(null, "Consensus failure on multisig deposit address")
      );
    }

    return `Multisig deposit address: ${results2[0].depositAddress}`;
  }

  async function queryMintBalance(mintAddress) {
    for (const x of publicSettings.authorityNodes) {
      process.stdout.write(
        `  ${getStyledAuthorityLink(x)} ${chalk.bold("->")} `
      );
      try {
        const result = splToken.validateSignedMessage(
          await post(`${getAuthorityLink(x)}/queryMintBalance`, {
            mintAddress: mintAddress,
          }),
          x.walletAddress
        );
        console.log(
          `mintedAmount: ${dingo.fromSatoshi(
            result.mintedAmount
          )}, depositedAmount: ${dingo.fromSatoshi(
            result.depositedAmount
          )}, unconfirmedAmount: ${dingo.fromSatoshi(
            result.unconfirmedAmount
          )}, depositAddress: ${result.depositAddress}`
        );
      } catch (error) {
        if (error.response) {
          console.log(
            getStyledError(error.response.statusCode, error.response.body)
          );
        } else {
          console.log(getStyledError(null, error.message));
        }
      }
    }
  }

  async function queryBurnHistory(burnAddress) {
    for (const x of publicSettings.authorityNodes) {
      process.stdout.write(
        `  ${getStyledAuthorityLink(x)} ${chalk.bold("->")} `
      );
      try {
        const result = splToken.validateSignedMessage(
          await post(`${getAuthorityLink(x)}/queryBurnHistory`, {
            burnAddress: burnAddress,
          }),
          x.walletAddress
        ).burnHistory;
        console.log();
        for (const i in result) {
          console.log(
            `    index: ${i}, amount: ${dingo.fromSatoshi(
              result[i].burnAmount
            )}, destination: ${result[i].burnDestination}, status: ${
              result[i].status
            }`
          );
        }
      } catch (error) {
        if (error.response) {
          console.log(
            getStyledError(error.response.statusCode, error.response.body)
          );
        } else {
          console.log(getStyledError(null, error.message));
        }
      }
    }
  }

  const executeMintHandler = async (test) => {
    let mintList = null;
    console.log("Retrieving pending mint...");
    for (const i in publicSettings.authorityNodes) {
      const node = publicSettings.authorityNodes[i];
      console.log(
        `  Requesting pending mint from Node ${i} at ${node.hostname} (${node.walletAddress})...`
      );
      const pendingMint = (
        await validateTimedAndSignedMessage(
          await post(
            `${getAuthorityLink(node)}/computePendingMint`,
            await createTimedAndSignedMessage({})
          ),
          node.walletAddress
        )
      ).pendingMint;
      for (const m of pendingMint) {
        console.log(
          `    ${m.mintAddress} -> ${dingo.fromSatoshi(m.mintAmount)}`
        );
      }
      if (mintList === null) {
        mintList = pendingMint;
      } else {
        mintList = mintList
          .map((x) => {
            const match = pendingMint.find(
              (y) =>
                y.mintAddress === x.mintAddress &&
                y.depositAddress === x.depositAddress
            );
            if (typeof match === "undefined") {
              return null;
            }
            return {
              mintAddress: x.mintAddress,
              depositAddress: x.depositAddress,
              mintAmount:
                BigInt(x.mintAmount) < BigInt(match.mintAmount)
                  ? x.mintAmount
                  : match.mintAmount,
            };
          })
          .filter((x) => x !== null);
      }
    }
    console.log("\n");

    console.log("Pending mint consensus = ");
    for (const m of mintList) {
      console.log(`    ${m.mintAddress} -> ${dingo.fromSatoshi(m.mintAmount)}`);
    }
    console.log("\n");

    if (mintList.length === 0) {
      console.log("Nothing to mint.");
      return;
    }
    const mint = mintList[0];
    console.log(`Minting: ${mint.mintAddress} -> ${dingo.fromSatoshi(mint.mintAmount)}\n\n`);

    console.log("Running test...");
    for (const i in publicSettings.authorityNodes) {
      const node = publicSettings.authorityNodes[i];
      console.log(
        `  Requesting approval from Node ${i} at ${node.hostname} (${node.walletAddress})...`
      );

      await validateTimedAndSignedMessage(
        await post(
          `${getAuthorityLink(node)}/approveMintTest`,
          await createTimedAndSignedMessage({
            mint: mint,
          })
        ),
        node.walletAddress
      );

      console.log("    -> Success!");
    }
    console.log("\n");

    if (!test) {
      console.log("Executing...");
      let signatures = [];
      for (const i in publicSettings.authorityNodes) {
        const node = publicSettings.authorityNodes[i];
        console.log(
          `  Requesting approval from Node ${i} at ${node.hostname} (${node.walletAddress})...`
        );

        const signature = (
          await validateTimedAndSignedMessage(
            await post(
              `${getAuthorityLink(node)}/approveMint`,
              await createTimedAndSignedMessage({
                mint: mint,
              })
            ),
            node.walletAddress
          )
        ).signature;
        signatures.push(signature);

        console.log("    -> Success!");
        console.log(signature);
      }
      console.log("\n");

      console.log(`  Sending finalized transaction...`);
      const txSignature = (
        await splToken.finalizeMintAndSend(
          mint.mintAddress,
          dingo.fromSatoshi(mint.mintAmount),
          signatures
        )
      ).signature;
      console.log(`  Success! Transaction signature: ${txSignature}`);
      console.log("\n");
    }
  };
  async function executeMint() {
    await executeMintHandler(false);
  }
  async function executeMintTest() {
    await executeMintHandler(true);
  }

  const executePayoutsHandler = async (
    processDeposits,
    processWithdrawals,
    test
  ) => {
    let depositTaxPayouts = null;
    let withdrawalPayouts = null;
    let withdrawalTaxPayouts = null;
    let unspent = null;

    console.log("Retrieving pending payouts...");
    for (const i in publicSettings.authorityNodes) {
      const node = publicSettings.authorityNodes[i];
      console.log(
        `  Requesting pending payouts from Node ${i} at ${node.hostname} (${node.walletAddress})...`
      );
      const {
        depositTaxPayouts: _depositTaxPayouts,
        withdrawalPayouts: _withdrawalPayouts,
        withdrawalTaxPayouts: _withdrawalTaxPayouts,
      } = await validateTimedAndSignedMessage(
        await post(
          `${getAuthorityLink(node)}/computePendingPayouts`,
          await createTimedAndSignedMessage({
            processDeposits: processDeposits,
            processWithdrawals: processWithdrawals,
          })
        ),
        node.walletAddress
      );
      const totalDepositTaxPayout = _depositTaxPayouts
        .reduce((a, b) => a + BigInt(b.amount), 0n)
        .toString();
      const totalWithdrawalPayout = _withdrawalPayouts
        .reduce((a, b) => a + BigInt(b.amount), 0n)
        .toString();
      const totalWithdrawalTaxPayout = _withdrawalTaxPayouts
        .reduce((a, b) => a + BigInt(b.amount), 0n)
        .toString();
      console.log(
        `    Total deposit tax = ${dingo.fromSatoshi(totalDepositTaxPayout)}`
      );
      for (const p of _depositTaxPayouts) {
        console.log(
          `      ${p.depositAddress} -> ${dingo.fromSatoshi(p.amount)}`
        );
      }
      console.log(
        `    Total withdrawal = ${dingo.fromSatoshi(totalWithdrawalPayout)}`
      );
      for (const p of _withdrawalPayouts) {
        console.log(
          `      ${p.burnDestination} -> ${dingo.fromSatoshi(p.amount)}`
        );
      }
      console.log(
        `    Total withdrawal tax = ${dingo.fromSatoshi(
          totalWithdrawalTaxPayout
        )}`
      );
      for (const p of _withdrawalTaxPayouts) {
        console.log(
          `      ${p.burnDestination} -> ${dingo.fromSatoshi(p.amount)}`
        );
      }

      if (depositTaxPayouts === null) {
        depositTaxPayouts = _depositTaxPayouts;
        withdrawalPayouts = _withdrawalPayouts;
        withdrawalTaxPayouts = _withdrawalTaxPayouts;
      } else {
        depositTaxPayouts = depositTaxPayouts.filter((x) =>
          _depositTaxPayouts.some(
            (y) =>
              y.depositAddress === x.depositAddress && y.amount === x.amount
          )
        );
        withdrawalPayouts = withdrawalPayouts.filter((x) =>
          _withdrawalPayouts.some(
            (y) =>
              y.burnAddress === x.burnAddress &&
              y.burnIndex === x.burnIndex &&
              y.burnDestination === x.burnDestination &&
              y.amount === x.amount
          )
        );
        withdrawalTaxPayouts = withdrawalTaxPayouts.filter((x) =>
          _withdrawalTaxPayouts.some(
            (y) =>
              y.burnAddress === x.burnAddress &&
              y.burnIndex === x.burnIndex &&
              y.burnDestination === x.burnDestination &&
              y.amount === x.amount
          )
        );
      }
    }
    console.log("\n");
    if (!processDeposits) {
      depositTaxPayouts = [];
    }
    if (!processWithdrawals) {
      withdrawalPayouts = [];
      withdrawalTaxPayouts = [];
    }

    console.log("Pending payouts consensus =");
    const totalDepositTaxPayout = depositTaxPayouts
      .reduce((a, b) => a + BigInt(b.amount), 0n)
      .toString();
    const totalWithdrawalPayout = withdrawalPayouts
      .reduce((a, b) => a + BigInt(b.amount), 0n)
      .toString();
    const totalWithdrawalTaxPayout = withdrawalTaxPayouts
      .reduce((a, b) => a + BigInt(b.amount), 0n)
      .toString();
    console.log(
      `  Total deposit tax = ${dingo.fromSatoshi(totalDepositTaxPayout)}`
    );
    for (const p of depositTaxPayouts) {
      console.log(`    ${p.depositAddress} -> ${dingo.fromSatoshi(p.amount)}`);
    }
    console.log(
      `  Total withdrawal = ${dingo.fromSatoshi(totalWithdrawalPayout)}`
    );
    for (const p of withdrawalPayouts) {
      console.log(`    ${p.burnDestination} -> ${dingo.fromSatoshi(p.amount)}`);
    }
    console.log(
      `  Total withdrawal tax = ${dingo.fromSatoshi(totalWithdrawalTaxPayout)}`
    );
    for (const p of withdrawalTaxPayouts) {
      console.log(`    ${p.burnDestination} -> ${dingo.fromSatoshi(p.amount)}`);
    }
    console.log("\n");

    console.log("Retrieving unspent...");
    for (const i in publicSettings.authorityNodes) {
      const node = publicSettings.authorityNodes[i];
      console.log(
        `  Requesting unspent from Node ${i} at ${node.hostname} (${node.walletAddress})...`
      );
      const { unspent: _unspent } = await validateTimedAndSignedMessage(
        await post(
          `${getAuthorityLink(node)}/computeUnspent`,
          await createTimedAndSignedMessage({})
        ),
        node.walletAddress
      );
      for (const u of _unspent) {
        console.log(`      ${u.txid} -> ${u.amount}`);
      }
      if (unspent === null) {
        unspent = _unspent;
      } else {
        unspent = unspent.filter((x) =>
          ((a) =>
            a.length === 1 &&
            dingo.toSatoshi(a[0].amount.toString()) ===
              dingo.toSatoshi(x.amount.toString()))(
            _unspent.filter((y) => y.txid === x.txid && y.vout === x.vout)
          )
        );
      }
    }
    console.log("\n");

    console.log("Unspent consensus = ");
    for (const u of unspent) {
      console.log(`    ${u.txid} -> ${u.amount}`);
    }
    console.log("\n");

    // Compute approval chain.
    let approvalChain = null;
    console.log(`Approval chain = \n${approvalChain}`);
    console.log("\n");

    console.log("Running test...");
    for (const i in publicSettings.authorityNodes) {
      const node = publicSettings.authorityNodes[i];
      console.log(
        `  Requesting approval from Node ${i} at ${node.hostname} (${node.walletAddress})...`
      );

      const approvalChainNext = (
        await validateTimedAndSignedMessage(
          await post(
            `${getAuthorityLink(node)}/approvePayoutsTest`,
            await createTimedAndSignedMessage({
              depositTaxPayouts: depositTaxPayouts,
              withdrawalPayouts: withdrawalPayouts,
              withdrawalTaxPayouts: withdrawalTaxPayouts,
              unspent: unspent,
              approvalChain: approvalChain,
            })
          ),
          node.walletAddress
        )
      ).approvalChain;

      console.log("    -> Success!");
      console.log(approvalChainNext);
    }
    console.log("\n");

    if (!test) {
      console.log("Executing...");
      for (const i in publicSettings.authorityNodes) {
        const node = publicSettings.authorityNodes[i];
        console.log(
          `  Requesting approval from Node ${i} at ${node.hostname} (${node.walletAddress})...`
        );

        const approvalChainNext = (
          await validateTimedAndSignedMessage(
            await post(
              `${getAuthorityLink(node)}/approvePayouts`,
              await createTimedAndSignedMessage({
                depositTaxPayouts: depositTaxPayouts,
                withdrawalPayouts: withdrawalPayouts,
                withdrawalTaxPayouts: withdrawalTaxPayouts,
                unspent: unspent,
                approvalChain: approvalChain,
              })
            ),
            node.walletAddress
          )
        ).approvalChain;

        approvalChain = approvalChainNext;
        console.log("    -> Success!");
        console.log(approvalChainNext);
      }

      console.log(`  Sending raw transaction:\n${approvalChain}`);
      const hash = await dingo.sendRawTranscation(approvalChain);
      console.log(`  Success! Transaction hash: ${hash}`);
      console.log("\n");
    }
  };
  async function executePayouts(processDeposits, processWithdrawals) {
    processDeposits = parseBool(processDeposits);
    processWithdrawals = parseBool(processWithdrawals);
    if (processDeposits === false && processWithdrawals === false) {
      throw new Error(
        "At least one of deposits or withdrawals must be processed"
      );
    }
    await executePayoutsHandler(processDeposits, processWithdrawals, false);
  }
  async function executePayoutsTest(processDeposits, processWithdrawals) {
    processDeposits = parseBool(processDeposits);
    processWithdrawals = parseBool(processWithdrawals);
    if (processDeposits === false && processWithdrawals === false) {
      throw new Error(
        "At least one of deposits or withdrawals must be processed"
      );
    }
    await executePayoutsHandler(processDeposits, processWithdrawals, true);
  }

  async function consensus() {
    const stats = [];
    for (const x of publicSettings.authorityNodes) {
      process.stdout.write(
        `  ${getStyledAuthorityLink(x)} ${chalk.bold("->")} `
      );
      try {
        const result = splToken.validateSignedMessage(
          await post(`${getAuthorityLink(x)}/stats`),
          x.walletAddress
        );
        stats.push(result);
        console.log("OK");
      } catch (error) {
        stats.push(undefined);
        if (error.response) {
          console.log(
            getStyledError(error.response.statusCode, error.response.body)
          );
        } else {
          console.log(getStyledError(null, error.message));
        }
      }
    }

    // Shared configurations.
    const dingoWidth = 20;

    function consensusCell(cell, columnIndex, rowIndex, rowData) {
      if (rowData.length === 0) {
        return this.style("YES", "bgGreen", "black");
      }

      let data = undefined;
      for (const row of rowData) {
        if (
          row[columnIndex] !== undefined &&
          row[columnIndex] !== null &&
          row[columnIndex] !== ""
        ) {
          if (data === undefined) {
            data = row[columnIndex];
          } else if (row[columnIndex] !== data) {
            return this.style("NO", "bgRed", "black");
          }
        }
      }
      return this.style("YES", "bgGreen", "black");
    }
    const nodeHeader = {
      alias: "Node",
      width: 11,
      formatter: function (x) {
        if (!x.startsWith("UNREACHABLE")) {
          return this.style(x, "bgWhite", "black");
        } else {
          return this.style(x.replace("UNREACHABLE", ""), "bgRed", "black");
        }
      },
    };

    function satoshiFormatter(x) {
      if (x === null || x === undefined || typeof x !== "string" || x === "") {
        return "";
      } else {
        return dingo.fromSatoshi(x);
      }
    }

    let s = "";

    // Version info.
    const versionFlattened = [];
    for (const i in stats) {
      const stat = stats[i];
      if (stat === undefined) {
        versionFlattened.push(["UNREACHABLE" + i, "", "", "", "", "", ""]);
      } else {
        versionFlattened.push([
          i,
          stat.version.repository.toString(),
          stat.version.hash.toString(),
          new Date(stat.version.timestamp).toUTCString(),
          stat.version.clean ? "Yes" : "No",
          stat.version.dingoVersion === undefined
            ? ""
            : stat.version.dingoVersion.toString(),
          stat.time === undefined ? "" : new Date(stat.time).toUTCString(),
        ]);
      }
    }
    const versionHeader = [
      nodeHeader,
      {
        alias: "Repository",
      },
      {
        alias: "Commit Hash",
      },
      {
        alias: "Commit Timestamp",
      },
      {
        alias: "Clean",
        formatter: function (x) {
          return x === "Yes"
            ? this.style("YES", "bgGreen", "black")
            : x === "No"
            ? this.style("NO", "bgRed", "black")
            : "";
        },
      },
      {
        alias: "Dingo Version",
      },
      {
        alias: "Stats Time",
      },
    ];
    const versionFooter = ["Consensus"]
      .concat(Array(3).fill(consensusCell))
      .concat([
        function (cell, columnIndex, rowIndex, rowData) {
          return "";
        },
      ])
      .concat(consensusCell)
      .concat([
        function (cell, columnIndex, rowIndex, rowData) {
          return "";
        },
      ]);
    s += "  [Version]";
    s += Table(versionHeader, versionFlattened, versionFooter).render();

    // Public Settings info.
    const publicSettingsFlattened = [];
    for (const i in stats) {
      const stat = stats[i];
      if (stat === undefined) {
        publicSettingsFlattened.push(["UNREACHABLE" + i, "", "", "", ""]);
      } else {
        try {
          publicSettingsFlattened.push([
            i,
            stat.publicSettings.payoutCoordinator.toString(),
            stat.publicSettings.authorityThreshold.toString(),
            stat.publicSettings.authorityNodes
              .map((x) => `${x.hostname}:${x.port}\\${x.walletAddress}`)
              .join(" "),
            stat.publicSettings.walletAddress,
          ]);
        } catch {
          publicSettingsFlattened.push([i, "", "", "", ""]);
        }
      }
    }
    const publicSettingsHeader = [
      nodeHeader,
      {
        alias: "Coordinator",
      },
      {
        alias: "Threshold",
      },
      {
        alias: "Authority Nodes",
        width: 80,
      },
      {
        alias: "Wallet Address",
      },
    ];
    const publicSettingsFooter = ["Consensus"]
      .concat(Array(publicSettingsHeader.length - 2).fill(consensusCell))
      .concat([
        function (cell, columnIndex, rowIndex, rowData) {
          return "";
        },
      ]);
    s += "\n\n  [Public Settings]";
    s += Table(
      publicSettingsHeader,
      publicSettingsFlattened,
      publicSettingsFooter
    ).render();

    // Dingo settings.
    const dingoSettingsFlattened = [];
    for (const i in stats) {
      const stat = stats[i];
      if (stat === undefined) {
        dingoSettingsFlattened.push(["UNREACHABLE" + i, "", "", "", ""]);
      } else {
        try {
          dingoSettingsFlattened.push([
            i,
            stat.dingoSettings.changeAddress,
            stat.dingoSettings.changeConfirmations.toString(),
            stat.dingoSettings.depositConfirmations.toString(),
            stat.dingoSettings.taxPayoutAddresses.join(" "),
          ]);
        } catch {
          dingoSettingsFlattened.push([i, "", "", "", ""]);
        }
      }
    }
    const dingoSettingsHeader = [
      nodeHeader,
      {
        alias: "Change Address",
      },
      {
        alias: "Change Confirmations",
      },
      {
        alias: "Deposit Confirmations",
      },
      {
        alias: "Tax Payout Addresses",
        width: 45,
      },
    ];
    const dingoSettingsFooter = ["Consensus"].concat(
      Array(dingoSettingsHeader.length - 1).fill(consensusCell)
    );
    s += "\n\n  [Dingo Settings]";
    s += Table(
      dingoSettingsHeader,
      dingoSettingsFlattened,
      dingoSettingsFooter
    ).render();

    // Smart contract settings.
    const solanaSettingsFlattened = [];
    for (const i in stats) {
      const stat = stats[i];
      if (stat === undefined) {
        solanaSettingsFlattened.push([
          "UNREACHABLE" + i,
          "",
          "",
          "",
          "",
          "",
          "",
          "",
        ]);
      } else {
        try {
          solanaSettingsFlattened.push([
            i,
            stat.solanaSettings.cluster,
            stat.solanaSettings.tokenAddress,
            stat.solanaSettings.multisigAccount,
            stat.solanaSettings.nonceAccount,
            stat.solanaSettings.feePayer,
            stat.solanaSettings.feePayer,
            stat.solanaSettings.mintDecimals,
          ]);
        } catch {
          solanaSettingsFlattened.push([i, "", "", "", "", "", "", ""]);
        }
      }
    }
    const solanaSettingsHeader = [
      nodeHeader,
      {
        alias: "Cluster",
      },
      {
        alias: "Token address",
      },
      {
        alias: "Multisig Address",
      },
      {
        alias: "Nonce Account",
      },
      {
        alias: "Nonce Authority",
      },
      {
        alias: "Fee Payer",
      },
      {
        alias: "Mint Decimals",
      },
    ];
    const solanaSettingsFooter = ["Consensus"].concat(
      Array(solanaSettingsHeader.length - 1).fill(consensusCell)
    );
    s += "\n\n  [Solana Settings]";
    s += Table(
      solanaSettingsHeader,
      solanaSettingsFlattened,
      solanaSettingsFooter
    ).render();

    // Confirmed deposits.
    const confirmedDepositStatsFlattened = [];
    for (const i in stats) {
      const stat = stats[i];
      if (stat === undefined) {
        confirmedDepositStatsFlattened.push([
          "UNREACHABLE" + i,
          "",
          "",
          "",
          "",
          "",
          "",
	  ""
        ]);
      } else {
        try {
          confirmedDepositStatsFlattened.push([
            i,
            stat.confirmedDeposits.count.toString(),
            stat.confirmedDeposits.totalDepositedAmount,
            stat.confirmedDeposits.totalApprovableAmount,
            stat.confirmedDeposits.totalApprovedAmount,
            stat.confirmedDeposits.remainingApprovableAmount,
            stat.confirmedDeposits.totalApprovableTax,
            stat.confirmedDeposits.totalApprovedTax,
            stat.confirmedDeposits.remainingApprovableTax,
          ]);
        } catch (e) {
          confirmedDepositStatsFlattened.push([i, "", "", "", "", "", "", ""]);
        }
      }
    }
    const confirmedDepositsHeader = [
      nodeHeader,
      {
        alias: "Addresses",
      },
      {
        alias: "Total Deposited",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approvable Amount",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approved Amount",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Remaining Amount",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approvable Tax",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approved Tax",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Remaining Tax",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
    ];
    const confirmedDepositsFooter = ["Consensus"].concat(
      Array(confirmedDepositsHeader.length - 1).fill(consensusCell)
    );
    s += "\n\n  [Deposits (Confirmed)]";
    s += Table(
      confirmedDepositsHeader,
      confirmedDepositStatsFlattened,
      confirmedDepositsFooter,
      {
        truncate: "...",
      }
    ).render();

    // Unconfirmed deposits.
    const unconfirmedDepositStatsFlattened = [];
    for (const i in stats) {
      const stat = stats[i];
      if (stat === undefined) {
        unconfirmedDepositStatsFlattened.push([
          "UNREACHABLE" + i,
          "",
          "",
          "",
          "",
          "",
          "",
          ""
        ]);
      } else {
        try {
          unconfirmedDepositStatsFlattened.push([
            i,
            stat.unconfirmedDeposits.count.toString(),
            stat.unconfirmedDeposits.totalDepositedAmount,
            stat.unconfirmedDeposits.totalApprovableAmount,
            stat.unconfirmedDeposits.totalApprovedAmount,
            stat.unconfirmedDeposits.remainingApprovableAmount,
            stat.unconfirmedDeposits.totalApprovableTax,
            stat.unconfirmedDeposits.totalApprovedTax,
            stat.unconfirmedDeposits.remainingApprovableTax
          ]);
        } catch (e) {
          unconfirmedDepositStatsFlattened.push([i, "", "", "", "", "", "", ""]);
        }
      }
    }
    const unconfirmedDepositsHeader = [
      nodeHeader,
      {
        alias: "Addresses",
      },
      {
        alias: "Total Deposited",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approvable Amount",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approved Amount",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Remaining Amount",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approvable Tax",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approved Tax",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Remaining Tax",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
    ];
    const unconfirmedDepositsFooter = ["Consensus"].concat(
      Array(unconfirmedDepositsHeader.length - 1).fill(consensusCell)
    );
    s += "\n\n  [Deposits (Unconfirmed + Confirmed)]";
    s += Table(
      unconfirmedDepositsHeader,
      unconfirmedDepositStatsFlattened,
      unconfirmedDepositsFooter,
      {
        truncate: "...",
      }
    ).render();

    // Withdrawals.
    const withdrawalStatsFlattened = [];
    for (const i in stats) {
      const stat = stats[i];
      if (stat === undefined) {
        withdrawalStatsFlattened.push([
          "UNREACHABLE" + i,
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          "",
        ]);
      } else {
        try {
          withdrawalStatsFlattened.push([
            i,
            stat.withdrawals.count.toString(),
            stat.withdrawals.totalBurnedAmount,
            stat.withdrawals.totalApprovableAmount,
            stat.withdrawals.totalApprovedAmount,
            stat.withdrawals.remainingApprovableAmount,
            stat.withdrawals.totalApprovableTax,
            stat.withdrawals.totalApprovedTax,
            stat.withdrawals.remainingApprovableTax,
          ]);
        } catch {
          withdrawalStatsFlattened.push([i, "", "", "", "", "", "", "", ""]);
        }
      }
    }
    const withdrawalHeader = [
      nodeHeader,
      {
        alias: "Submissions",
      },
      {
        alias: "Total Burned",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approvable Amount",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approved Amount",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Remaining Amount",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approvable Tax",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Approved Tax",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Remaining Tax",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
    ];
    const withdrawalFooter = ["Consensus"].concat(
      Array(withdrawalHeader.length - 1).fill(consensusCell)
    );
    s += "\n\n  [Submitted Withdrawals]";
    s += Table(withdrawalHeader, withdrawalStatsFlattened, withdrawalFooter, {
      truncate: "...",
    }).render();

    // UTXOs.
    const confirmedUtxoStatsFlattened = [];
    for (const i in stats) {
      const stat = stats[i];
      if (stat === undefined) {
        confirmedUtxoStatsFlattened.push(["UNREACHABLE" + i, "", ""]);
      } else {
        try {
          confirmedUtxoStatsFlattened.push([
            i,
            stat.confirmedUtxos.totalChangeBalance,
            stat.confirmedUtxos.totalDepositsBalance,
          ]);
        } catch (e) {
          confirmedUtxoStatsFlattened.push([i, "", ""]);
        }
      }
    }
    const confirmedUtxoHeader = [
      nodeHeader,
      {
        alias: "Change Balance",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Deposits Balance",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
    ];
    const confirmedUtxoFooter = ["Consensus"].concat(
      Array(confirmedUtxoHeader.length - 1).fill(consensusCell)
    );
    s += "\n\n  [UTXOs (Confirmed)]";
    s += Table(
      confirmedUtxoHeader,
      confirmedUtxoStatsFlattened,
      confirmedUtxoFooter,
      {
        truncate: "...",
      }
    ).render();

    // UTXOs.
    const unconfirmedUtxoStatsFlattened = [];
    for (const i in stats) {
      const stat = stats[i];
      if (stat === undefined) {
        unconfirmedUtxoStatsFlattened.push(["UNREACHABLE" + i, "", ""]);
      } else {
        try {
          unconfirmedUtxoStatsFlattened.push([
            i,
            stat.unconfirmedUtxos.totalChangeBalance,
            stat.unconfirmedUtxos.totalDepositsBalance,
          ]);
        } catch (e) {
          unconfirmedUtxoStatsFlattened.push([i, "", ""]);
        }
      }
    }
    const unconfirmedUtxoHeader = [
      nodeHeader,
      {
        alias: "Change Balance",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
      {
        alias: "Deposits Balance",
        formatter: satoshiFormatter,
        width: dingoWidth,
      },
    ];
    const unconfirmedUtxoFooter = ["Consensus"].concat(
      Array(unconfirmedUtxoHeader.length - 1).fill(consensusCell)
    );
    s += "\n\n  [UTXOs (Unconfirmed + Confirmed)]";
    s += Table(
      unconfirmedUtxoHeader,
      unconfirmedUtxoStatsFlattened,
      unconfirmedUtxoFooter,
      {
        truncate: "...",
      }
    ).render();

    console.log(s);
  }

  async function log(index) {
    const result = await post(
      `${getAuthorityLink(publicSettings.authorityNodes[parseInt(index)])}/log`,
      await createTimedAndSignedMessage({})
    );
    console.log(result.log);
  }

  async function syncDatabase(index) {
    console.log("Downloading database...");
    const result = await post(
      `${getAuthorityLink(
        publicSettings.authorityNodes[parseInt(index)]
      )}/dumpDatabase`,
      await createTimedAndSignedMessage({})
    );
    console.log("Overwriting local database...");
    await database.reset(databaseSettings.databasePath, result.sql);
    console.log("Done!");
  }

  async function dingoDoesAHarakiri(index, ...message) {
    console.log("Message = " + message.join(" "));
    console.log("Sending suicide signal to nodes...");
    for (const x of index === undefined
      ? publicSettings.authorityNodes
      : [publicSettings.authorityNodes[parseInt(index)]]) {
      process.stdout.write(
        `  ${getStyledAuthorityLink(x)} ${chalk.bold("->")} `
      );
      try {
        await post(
          `${getAuthorityLink(x)}/dingoDoesAHarakiri`,
          await createTimedAndSignedMessage({ message: message.join(" ") })
        );
        console.log("OK");
      } catch (error) {
        if (error.response) {
          console.log(
            getStyledError(error.response.statusCode, error.response.body)
          );
        } else {
          console.log(getStyledError(null, error.message));
        }
      }
    }
  }
})();
