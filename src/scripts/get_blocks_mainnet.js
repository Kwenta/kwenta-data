import EthDater from 'ethereum-block-by-date';
import { ethers } from 'ethers';
import 'dotenv/config';
import fs from 'fs';

// set some constants
const NETWORK = 'optimism-mainnet'; // mainnet

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
        '2022-07-08T12:00:00Z',
        1,
        true,
        false
    );
    console.log('Blocks: ', blocks);
    
    console.log('Writing blocks...');
    fs.writeFile(
        './data/blocks_mainnet.json',
        JSON.stringify(blocks),
        (err) => {
            if(err) throw err;
            console.log('Blocks written!');
        }    
    );
})();
