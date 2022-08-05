import EthDater from 'ethereum-block-by-date';
import { ethers } from 'ethers';
import 'dotenv/config';
import fs from 'fs';

// set some constants
const NETWORK = 'optimism-kovan'; // testnet

// set up the objects
const provider = new ethers.providers.JsonRpcProvider(
    `https://${NETWORK}.infura.io/v3/${process.env.INFURA_KEY}`
);

const dater = new EthDater(provider);

// get the blocks and log to console
(async () => {
    console.log('Getting blocks...');
    const today = new Date();

    let blocks = await dater.getEvery(
        'days',
        '2022-07-01T00:00:00Z',
        today.toISOString(),
        1,
        true,
        false
    );
    console.log('Blocks: ', blocks);
    
    console.log('Writing blocks...');
    fs.writeFile(
        './data/blocks_testnet.json',
        JSON.stringify(blocks),
        (err) => {
            if(err) throw err;
            console.log('Blocks written!');
        }    
    );
})();
