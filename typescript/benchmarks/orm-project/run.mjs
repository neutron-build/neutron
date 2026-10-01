import {mkdir,cp,writeFile,readFile,statfs} from 'node:fs/promises';
import {resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
const runtime=resolve(process.env.ORM_RUNTIME_DIR??'');
if(!process.env.ORM_RUNTIME_DIR||runtime===process.cwd())throw Error('Explicit private ORM_RUNTIME_DIR required');
const source=dirname(fileURLToPath(import.meta.url));
const repo=spawnSync('git',['rev-parse','--show-toplevel'],{cwd:source,encoding:'utf8'}).stdout.trim();if(repo&&(runtime===repo||runtime.startsWith(repo+'/')))throw Error('Runtime must be outside tracked repository');
if(process.env.ORM_OUTPUT_DIR){const output=resolve(process.env.ORM_OUTPUT_DIR);if(repo&&(output===repo||output.startsWith(repo+'/')))throw Error('Output must be outside tracked repository');}
const space=await statfs(dirname(runtime));if(space.bavail*space.bsize<6*1024**3)throw Error('6GiB disk guard');
await mkdir(runtime,{recursive:true});
const manifest={private:true,type:'module',dependencies:{'@neutron-build/sql':'0.1.0',prisma:'7.10.0','@prisma/client':'7.10.0','@prisma/adapter-pg':'7.10.0','drizzle-orm':'0.45.3',pg:'8.23.1'}};
const install=process.argv.includes('--install');
let existing;try{existing=JSON.parse(await readFile(`${runtime}/package.json`));}catch(e){if(e.code!=='ENOENT')throw e;}
if(existing&&JSON.stringify(existing.dependencies)!==JSON.stringify(manifest.dependencies))throw Error('Refuse unrelated runtime manifest');
let hasLock=false;try{await readFile(`${runtime}/package-lock.json`);hasLock=true;}catch(e){if(e.code!=='ENOENT')throw e;}
if(install){await writeFile(`${runtime}/package.json`,JSON.stringify(manifest,null,2)+'\n');const r=spawnSync('npm',[hasLock?'ci':'install','--no-audit','--no-fund'],{cwd:runtime,stdio:'inherit'});if(r.status!==0)process.exit(r.status??1);}
const actual=JSON.parse(await readFile(`${runtime}/package.json`));for(const [name,version]of Object.entries(manifest.dependencies))if(actual.dependencies[name]!==version)throw Error(`runtime pin mismatch ${name}`);
for(const [name,version]of Object.entries(manifest.dependencies)){const pkg=JSON.parse(await readFile(`${runtime}/node_modules/${name}/package.json`));if(pkg.version!==version)throw Error(`installed version mismatch ${name}`);}
const after=await statfs(runtime);if(after.bavail*after.bsize<6*1024**3)throw Error('6GiB disk guard after install');
await mkdir(`${runtime}/src`,{recursive:true});for(const file of ['providers.mjs','main.mjs','schema.prisma'])await cp(`${source}/${file}`,`${runtime}/src/${file}`);
const generated=spawnSync(`${runtime}/node_modules/.bin/prisma`,['generate','--schema','src/schema.prisma'],{cwd:runtime,stdio:'inherit'});if(generated.status!==0)process.exit(generated.status??1);
const result=spawnSync(process.execPath,['src/main.mjs'],{cwd:runtime,stdio:'inherit',env:process.env});process.exit(result.status??1);
