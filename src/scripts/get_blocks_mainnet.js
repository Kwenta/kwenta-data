import EthDater from 'ethereum-block-by-date';
import { ethers } from 'ethers';
import axios from 'axios';
import 'dotenv/config';
import fs from 'fs';

// set some constants
const NETWORK = 'optimism-mainnet'; // mainnet
const DIR_NAME = 'data';
const FILE_NAME = 'blocks_mainnet.json';
const PERIOD = 3;
const PERIOD_SECONDS = PERIOD * 60 * 60 * 1000;

// set up the objects
const provider = new ethers.providers.JsonRpcProvider(
    `https://${NETWORK}.infura.io/v3/${process.env.INFURA_KEY}`
);

const dater = new EthDater(provider);

// get the blocks and write to local directory
(async () => {
    // pull the existing file from fleek
    const existingBlocksRequest = await axios.get(
        `${process.env.FLEEK_BASE_URL}/${DIR_NAME}/${FILE_NAME}`
    );
    const existingBlocks = existingBlocksRequest?.data ?? [];
    // set date boundaries
    let firstDate;
    if (existingBlocksRequest.status === 200 && existingBlocksRequest.data.length > 0) {
        firstDate = new Date(existingBlocks[existingBlocks.length - 1].date);
    } else {
        firstDate = new Date('2022-08-01T00:00:00Z');
    }
    const startTime = new Date(firstDate.setTime(firstDate.getTime() + PERIOD_SECONDS)).toISOString();
    const endTime = new Date().toISOString();

    console.log(`Getting blocks from ${startTime} to ${endTime}`);


    let newBlocks = await dater.getEvery(
        'hours',
        startTime,
        endTime,
        PERIOD,
        true,
        false
    );

    newBlocks = newBlocks.map(({ date, timestamp, block }) => {
        const ts = new Date(date);
        return {
            date,
            block,
            tsBlock: timestamp,
            ts: ts.getTime()
        }
    })

    const blocks = [
        ...existingBlocks,
        ...newBlocks
    ];
    console.log(`Blocks received: ${blocks.length}`);

    if (!fs.existsSync(`./${DIR_NAME}`)) {
        console.log('Creating directory...');
        fs.mkdirSync(`./${DIR_NAME}`);
    }
    
    console.log('Writing blocks...');
    fs.writeFile(
        `${DIR_NAME}/${FILE_NAME}`,
        JSON.stringify(blocks),
        (err) => {
            if(err) throw err;
            console.log('Blocks written!');
        }    
    );
})();
