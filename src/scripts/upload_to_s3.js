import 'dotenv/config'
import fs from 'node:fs'
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'

const REGION = 'us-east-1'
const EXCHANGE_STATS_BUCKET = 'synthetix-exchange-stats'

const s3Client = new S3Client({
	region: REGION,
	credentials: {
		accessKeyId: `${process.env.AWS_ACCESS_KEY_ID}`,
		secretAccessKey: `${process.env.AWS_SECRET_ACCESS_KEY}`,
	},
})

async function uploadFilesToS3(path, filename) {
	const fileContent = fs.readFileSync(path)
	
	const uploadParams = {
		Bucket: EXCHANGE_STATS_BUCKET,
		Key: filename,
		Body: fileContent,
		ContentType: 'application/json',
	}

	const command = new PutObjectCommand(uploadParams)
	return s3Client.send(command)
}

const files = [
	'daily_stats.json',
	'daily_stats_v3.json',
]

for (const file of files) {
	try {
		console.log(`Uploading ${file}...`)
		await uploadFilesToS3(`data/stats/${file}`, file)
		console.log(`Successfully uploaded ${file}`)
	} catch (error) {
		console.error(`Failed to upload file ${file}: ${error}`)
		throw error
	}
}



