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
    const today = new Date();

    let blocks = await dater.getEvery(
        'days',
        '2022-08-01T00:00:00Z',
        today.toISOString(),
        1,
        true,
        false
    );

    blocks = blocks.map(({ date, timestamp, block }) => {
        const ts = new Date(date);
        return {
            date,
            block,
            tsBlock: timestamp,
            ts: ts.getTime()
        }
    })
    console.log(`Blocks received: ${blocks.length}`);
    
    console.log('Writing blocks...');
    var dir = './data';

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }

    fs.writeFile(
        `${dir}/blocks_mainnet.json`,
        JSON.stringify(blocks),
        (err) => {
            if(err) throw err;
            console.log('Blocks written!');
        }    
    );
})();
