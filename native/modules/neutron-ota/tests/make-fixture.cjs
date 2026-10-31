// Deterministic public test vector; RFC 8032 test seed, never a release signing key.
const { createPrivateKey, createPublicKey, sign, createHash } = require('node:crypto');
const fs = require('node:fs'), path = require('node:path');
const root = path.join(__dirname, 'fixtures');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const old = Buffer.from('global.__NEUTRON_OTA_TEST__=0;\n'), next = Buffer.from('global.__NEUTRON_OTA_TEST__=1;\n'), retained = Buffer.from('retained asset\n');
const digest = sha(Buffer.from(['assets/retained.txt\0'+sha(retained)+'\n', 'index.jsbundle\0'+sha(next)+'\n'].join('')));
const manifest = { id: 'signed-fixture-1', version: '1.0.1', buildNumber: 1, runtimeVersion: '0.76.0', channel: 'test', bundleHash: digest, downloadSize: next.length, createdAt: '2026-10-07T00:00:00Z', minAppVersion: null,
  chunks: [{ path: 'index.jsbundle', hash: sha(next), size: next.length, url: 'https://fixture.invalid/index.jsbundle', operation: 'modify' }, { path: 'obsolete.txt', hash: '', size: 0, url: '', operation: 'delete' }] };
const canonical = JSON.stringify(manifest);
const seed = Buffer.from('9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60', 'hex');
const privateKey = createPrivateKey({ key: Buffer.concat([Buffer.from('302e020100300506032b657004220420','hex'),seed]), format: 'der', type: 'pkcs8' });
const publicKey = createPublicKey(privateKey), der = publicKey.export({ format: 'der', type: 'spki' });
const fixture = { publicKey: der.subarray(12).toString('base64'), publicKeyPEM: publicKey.export({format:'pem',type:'spki'}).toString(), canonical, manifest: { ...manifest, signature: sign(null, Buffer.from(canonical), privateKey).toString('base64') }, chunkBase64: next.toString('base64') };
fs.mkdirSync(path.join(root,'packaged/assets'), {recursive:true});
fs.writeFileSync(path.join(root,'vector.json'), JSON.stringify(fixture,null,2)+'\n');
fs.writeFileSync(path.join(root,'packaged/index.jsbundle'), old); fs.writeFileSync(path.join(root,'packaged/assets/retained.txt'), retained); fs.writeFileSync(path.join(root,'packaged/obsolete.txt'),'delete me\n');
