const _ = require('lodash');
const express = require('express');
const sdk = require('sc2-sdk');
const bodyParser = require('body-parser');
const mongoClient = require('./mongo_utils')
const utils = require('./utils');
var Block = require('./models/block').Block;
var ValidationError = require('mongoose/lib/error/validation')
const sc2 = sdk.Initialize({ app: 'steemlinked.app' });
const port = process.env.PORT || 4000;
const cache = {};
const useCache = false;

const limit = 50;

const clearGC = () => {
  try {
    global.gc();
  } catch (e) {
    console.log("You must run program with 'node --expose-gc index.js' or 'npm start'");
  }
};

setInterval(clearGC, 60 * 1000);
  
/** Stream the blockchain for Transactions */

 
/** Stream the blockchain for Transactions */

const getTransactions = ops => {
  //console.log("in getTxn getting txns")
  ops.forEach(op => {
    const type = op.op[0];
    const params = op.op[1];
    switch (type) {
      case 'transfer' || 'vote': {
        /** Find transfer */
        const transaction = {
          type: 'transfer',
          from: params.from,
          amount: params.amount,
          memo: params.memo,
          to:params.to,
          timestamp: Date.parse(op.timestamp) / 1000,
          block: op.block,
        };
        //store to mongodb if from is swapsteem
        if(transaction.to ==='swapsteem'){
         console.log('Transfer : ', JSON.stringify(op));
        }
        break;
        /** Add Cases for all transactionTYPES */
        
      }
    }
  });
  return true

};

const loadBlock = blockNum => {
  console.log('in blocknum and loading block Num '+blockNum)
  utils
    .getOpsInBlock(blockNum, false)
    .then(ops => {
      if (!ops.length) {
       console.error('in here with ', blockNum);
        console.error('Block does not exit?', blockNum);
        utils
          .getBlock(blockNum)
          .then(block => {
            if (block && block.previous && block.transactions.length === 0) {
              console.log('Block exist and is empty, load next', blockNum);
               } else {
              console.log('Sleep and retry', blockNum);
              utils.sleep(2000).then(() => {
                loadBlock(blockNum);
              });
            }
          })
          .catch(err => {
            console.log(
              'Error lightrpc (getBlock), sleep and retry',
              blockNum,
              JSON.stringify(err),
            );
            utils.sleep(2000).then(() => {
              loadBlock(blockNum);
            });
          });
      } else {
        console.log
       Block.findOneAndUpdateAsync({},{'blockNum':blockNum})
       .then(function (block) {
           console.log("Saving block no :"+block)
           var flag = getTransactions(ops);
           flag === true ? loadNextBlock():loadBlock(blockNum)
         }).catch(ValidationError, function(err) {
           err.logError = false
           err.productionMessage = true
           throw err
         });
        }
          })
    .catch(err => {
      console.error('Call failed with lightrpc (getOpsInBlock)', err);
      console.log('Retry', blockNum);
      loadBlock(blockNum);
    });
};

const loadNextBlock = () => {
    Block.findOne().lean().then(function(foundItems){
      console.log("last_block num : " +foundItems)

      let nextBlockNum = null;
      if(foundItems!==null){
       nextBlockNum = parseInt(foundItems.blockNum)+1;
       console.log("Now processing next block : #"+nextBlockNum)
      }else{
       nextBlockNum =  31913550  ;// testing purpose
      }
     utils
       .getGlobalProps()
       .then(globalProps => {
         console.log("globalProps "+globalProps)
         const lastIrreversibleBlockNum = globalProps.last_irreversible_block_num;
         if (lastIrreversibleBlockNum >= nextBlockNum) {
           console.log("calling loadblock()"+nextBlockNum)
           loadBlock(nextBlockNum);
           console.log("calling loadblock() done "+nextBlockNum)
         } else {
           utils.sleep(2000).then(() => {
             console.log(
               'Waiting to be on the lastIrreversibleBlockNum',
               lastIrreversibleBlockNum,
               'now nextBlockNum',
               nextBlockNum,
             );
             loadNextBlock();
           });
         }
       })
       .catch(err => {
         console.error('Call failed with lightrpc (getGlobalProps)', err);
         utils.sleep(2000).then(() => {
           console.log('Retry loadNextBlock', nextBlockNum);
           loadNextBlock();
         });
       });
     })
   }

const start = () => {
  console.info('Start streaming blockchain');
  loadNextBlock();
};

start();


