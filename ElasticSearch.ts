import * as logger from './Log'

const AWS = require('aws-sdk')
const path = require('path')

class ES {
    public doctype: string
    public index: string
    private readonly region: string
    private readonly endpoint: any
    private readonly creds: string
    constructor (region: string, endpointForES: string, index: string, doctype: string) {
        this.region = region
        this.endpoint = endpointForES
        this.index = index
        this.doctype = doctype
        this.endpoint = new AWS.Endpoint(endpointForES) // AWS Endpoint from created ES Domain Endpoint
        this.creds = new AWS.EnvironmentCredentials('AWS') // The AWS credentials are picked up from the environment.
    }
    public put (id: any, doc: {}, context: any) {
        return new Promise((resolve, reject) => {
            const req = new AWS.HttpRequest(this.endpoint)
            req.method = 'PUT'
            req.path = path.join('/', this.index, this.doctype, id)
            req.region = this.region
            req.headers['presigned-expires'] = false
            req.headers['Host'] = this.endpoint.host
            req.headers['Content-Type'] = 'application/json'
            req.body = doc

            const signer = new AWS.Signers.V4(req , 'es')  // es: service code
            signer.addAuthorization(this.creds, new Date())
            const send = new AWS.NodeHttpClient()
            send.handleRequest(req, null, (httpResp: { on: (arg0: string, arg1: { (chunk: any): void; (chunk: any): void }) => void }) => {
                let respBody = ''
                httpResp.on('data', (chunk: string) => {
                    respBody += chunk
                })
                httpResp.on('end', () => {
                    resolve(respBody)
                })
            }, (err: any) => {
                logger.debug(`Failed to put a record with id ${id}`, doc)
                reject(err)
            })
        })

    }
    public putBulk (doc: {}, context: any) {
        return new Promise((resolve, reject) => {
            const req = new AWS.HttpRequest(this.endpoint)
            req.method = 'PUT'
            req.path = path.join('/', this.index, this.doctype, '_bulk', '?refresh=wait_for')
            req.region = this.region
            req.headers['presigned-expires'] = false
            req.headers['Host'] = this.endpoint.host
            req.headers['Content-Type'] = 'application/json'
            req.body = doc

            const signer = new AWS.Signers.V4(req , 'es')  // es: service code
            signer.addAuthorization(this.creds, new Date())
            const send = new AWS.NodeHttpClient()

            send.handleRequest(req, null, (httpResp: { on: (arg0: string, arg1: { (chunk: any): void; (chunk: any): void }) => void }) => {
                let respBody = ''
                httpResp.on('data', (chunk: string) => {
                    respBody += chunk
                })
                httpResp.on('end', () => {
                    if (JSON.parse(respBody).errors) {
                        logger.warn(`ESERROR`, JSON.parse(respBody), {} as any)
                    }
                    resolve(respBody)
                })
            }, (err: any) => {
                logger.debug('Failed to put bulk records', doc)
                reject(err)
            })
        })
    }

}

export { ES }
