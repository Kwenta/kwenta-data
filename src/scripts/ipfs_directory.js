import { globSource } from 'ipfs-core';
import fleekStorage from '@fleekhq/fleek-storage-js';
import 'dotenv/config';
import fs from 'fs';

// configs
const globSourceOptions = {
};

const DEFAULT_FLEEK_CONFIG = {
    apiKey: `${process.env.FLEEK_API_KEY}`,
    apiSecret: `${process.env.FLEEK_API_SECRET}`,
    // httpUploadProgressCallback: (event) => {
    //     console.log(Math.round(event.loaded / event.total * 100) + '% done');
    // }
}

// upload the files
for await (const source of globSource('./data', '**/*', globSourceOptions)) {
    console.log('Raw filename: ', source.path)
    fs.readFile(`./data/${source.path}`, async (error, fileData) => {
        const uploadedFile = await fleekStorage.upload({
            key: `data${source.path}`,
            data: fileData,
            ...DEFAULT_FLEEK_CONFIG
        }).then((result) => {
            console.log('Result: ', result)
        }).catch((err) => {
            console.error(err)
        });
    })
}
