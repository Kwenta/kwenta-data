import { create, globSource } from 'ipfs-core'

const ipfs = await create()

// configs
const globSourceOptions = {
};

const addOptions = {
    pin: true,
    wrapWithDirectory: true,
    timeout: 10000
};

const cpOptions = {
    create: true
};

var uploadReturn = [];
for await (const file of ipfs.addAll(globSource('./data', '**/*', globSourceOptions), addOptions)) {
    uploadReturn.push(file);
}

// get the directory
const directory = uploadReturn.find((value) => {
    return value.path === '';
})

const files = uploadReturn.filter((value) => {
    return value.path !== '';
})

console.log(directory)
console.log(files)

// check status
try {
    const dirStatus = await ipfs.files.stat('/kwenta')
    console.log(dirStatus)
} catch(err) {
    // if no directory, make one
    const mkDir = await ipfs.files.mkdir('/kwenta')
    console.log(mkDir)
}

// copy files to directory
const cpDir = await ipfs.files.cp(`/ipfs/${directory.cid.toString()}`, '/kwenta', cpOptions)
console.log(cpDir)

// rename directory
const mvDir = await ipfs.files.mv(`/kwenta/${directory.cid.toString()}`, '/kwenta/data', cpOptions)
console.log(mvDir)

// list files in the directory
const result = [];
for await (const resultPart of ipfs.files.ls('/kwenta/data')) {
    result.push(resultPart)
}
console.log(result)

// stop the client
ipfs.stop();