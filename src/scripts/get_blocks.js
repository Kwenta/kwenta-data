const EthDater = require('ethereum-block-by-date');
const { ethers } = require('ethers');
var fs = require('fs');

// set some constants
// const NETWORK = 'optimism-mainnet'; // mainnet
const NETWORK = 'optimism-kovan'; // testnet


// set up the objects
const provider = new ethers.providers.JsonRpcProvider(
    `https://${NETWORK}.infura.io/v3/${process.env.INFURA_KEY}`
);

const dater = new EthDater(provider);

// get the blocks and log to console
(async () => {
    console.log('Getting blocks...');
    let blocks = await dater.getEvery(
        'weeks',
        '2022-01-01T12:00:00Z',
        '2022-06-13T12:00:00Z',
        1,
        true,
        false
    );
    console.log('Blocks: ', blocks);
    
    console.log('Writing blocks...');
    fs.writeFile(
        './data/blocks.json',
        JSON.stringify(blocks),
        (err) => {
            if(err) throw err;
            console.log('Blocks written!');
        }    
    );
})();
