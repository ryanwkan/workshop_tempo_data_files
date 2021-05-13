const middy = require('middy')
const sampleLogging = require('./middleware/sample-logging')

import { Context } from 'aws-lambda'
import { ES } from './lib/ElasticSearch'
import * as log from './lib/Log'
const ndjson = require('ndjson')
const { REGION, TEMPO_DATA_INDEX, NOTIFICATION_ES_ENDPOINT } = process.env

async function sampleLogger (event: any, context: Context): Promise<void> {
    // just to get started you can log the event, this will cause issues
    // if you start using API gateway with binary content.
    console.log(`event: ${JSON.stringify(event)}`)

    try {
        const elasticSearch = new ES(
            (REGION as string),
            (NOTIFICATION_ES_ENDPOINT as string),
            (TEMPO_DATA_INDEX as string),
            'event'
        )

        let doc = ''
        const serialize = ndjson.serialize()

        serialize.on('data', (line: string) => {
            doc += line
        })

        if (event.Records) {
            event.Records.map((record: any) => {
                const recordBody = JSON.parse(JSON.parse(record.body).Message)
                console.log(`recordBody: ${JSON.stringify(recordBody)}`)
                const curatorFormattedTimestamp = recordBody.submissionTime.substr(0, 10).replace(/-/g, '.', )
                // @ts-ignore // format => ${elasticSearch.index}-2017.01.31
                serialize.write({ index :
                        {
                            _index : `${elasticSearch.index}-${curatorFormattedTimestamp}`,
                            _type : elasticSearch.doctype,
                            _id : recordBody.triggerChainId
                        }
                })
                serialize.write(recordBody)
            })
        }
        serialize.end()

        const res = await elasticSearch.putBulk(doc, context)
        console.log(`res: ${JSON.stringify(res)}`)
    } catch (err) {
        log.error('lambda handler failed', { event } as any, err)
        throw err
    }
}

export const handler = middy(sampleLogger)
    .use(sampleLogging({ sampleRate: 0.5 }))
