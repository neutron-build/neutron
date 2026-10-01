//! Optional real PostgreSQL migration boundary regressions.
//! Set NEUTRON_TEST_DATABASE_URL to a disposable PostgreSQL admin URL.
//! Each test owns a fresh v10_rust_containment_* database and drops it.
use neutron_postgres::{migrate, PgPool};
use std::{
    error::Error,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_postgres::{Client, NoTls};
type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const CLI_KEY: i64 = 7043516000858342567;
const BARRIER: i64 = 81725024993;
const LEDGER: &str = "__pg_migrations";
const FOREIGN: &str = "__nucleus_migrations";
fn ensure(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
fn admin_url() -> Option<String> {
    match std::env::var("NEUTRON_TEST_DATABASE_URL") {
        Ok(url) => Some(url),
        Err(_) => {
            eprintln!("NEUTRON_TEST_DATABASE_URL absent; PostgreSQL regression skipped");
            None
        }
    }
}
fn make_pool(cfg: &tokio_postgres::Config) -> PgPool {
    PgPool::new(
        neutron_postgres::PgConfig::new()
            .host(match &cfg.get_hosts()[0] {
                tokio_postgres::config::Host::Tcp(host) => host.clone(),
                _ => panic!("TCP test URL required"),
            })
            .port(cfg.get_ports().first().copied().unwrap_or(5432))
            .dbname(cfg.get_dbname().unwrap_or("postgres"))
            .user(cfg.get_user().unwrap_or("postgres"))
            .password(String::from_utf8_lossy(cfg.get_password().unwrap_or(&[])))
            .max_size(1),
    )
}
struct Fixture {
    config: tokio_postgres::Config,
    admin: Client,
    control: Client,
    admin_driver: JoinHandle<()>,
    control_driver: JoinHandle<()>,
    pool: PgPool,
    name: String,
    dir: PathBuf,
}
impl Fixture {
    async fn new(url: &str) -> TestResult<Self> {
        let (admin, connection) = tokio_postgres::connect(url, NoTls).await?;
        let admin_driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let name = format!(
            "v10_rust_containment_pg_{}_{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await?;
        let mut cfg: tokio_postgres::Config = url.parse()?;
        cfg.dbname(&name);
        let (control, connection) = match cfg.connect(NoTls).await {
            Ok(result) => result,
            Err(error) => {
                admin
                    .batch_execute(&format!("DROP DATABASE {name} WITH(FORCE)"))
                    .await?;
                return Err(error.into());
            }
        };
        let control_driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let dir = std::env::temp_dir().join(&name);
        fs::create_dir(&dir)?;
        let pool = make_pool(&cfg);
        Ok(Self {
            config: cfg,
            admin,
            control,
            admin_driver,
            control_driver,
            pool,
            name,
            dir,
        })
    }
    async fn cleanup(&self) -> TestResult {
        self.admin
            .batch_execute(&format!("DROP DATABASE {} WITH(FORCE)", self.name))
            .await?;
        Ok(())
    }
    async fn lock(&self, key: i64) -> TestResult {
        self.control
            .query_one("SELECT pg_catalog.pg_advisory_lock($1)", &[&key])
            .await?;
        Ok(())
    }
    async fn unlock(&self, key: i64) -> TestResult {
        let row = self
            .control
            .query_one("SELECT pg_catalog.pg_advisory_unlock($1)", &[&key])
            .await?;
        ensure(row.try_get(0)?, "controller unlock unconfirmed")
    }
    async fn wait(&self, key: i64, pid: Option<i32>) -> TestResult<i32> {
        timeout(Duration::from_secs(8),async {
            loop {
                let rows=self.control.query("SELECT l.pid FROM pg_catalog.pg_locks l JOIN pg_catalog.pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory' AND NOT l.granted AND a.datname=$1 AND ((l.classid::bigint<<32)|l.objid::bigint)=$2",&[&self.name,&key]).await?;
                for row in rows {
                    let waiter:i32=row.try_get(0)?;
                    if pid.is_none()||pid==Some(waiter) {return Ok::<_,Box<dyn Error+Send+Sync>>(waiter);}
                }
                sleep(Duration::from_millis(10)).await;
            }
        }).await?
    }
    fn run(&self) -> JoinHandle<Result<(), neutron_postgres::PgError>> {
        let pool = self.pool.clone();
        let dir = self.dir.clone();
        tokio::spawn(async move { migrate(&pool, dir).await })
    }
    async fn business_exists(&self) -> TestResult<bool> {
        Ok(self
            .control
            .query_one(
                "SELECT pg_catalog.to_regclass('public.probe') IS NOT NULL",
                &[],
            )
            .await?
            .try_get(0)?)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin_driver.abort();
        self.control_driver.abort();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn competing_runs_are_serialized() -> TestResult {
    let Some(url) = admin_url() else {
        return Ok(());
    };
    let fixture = Fixture::new(&url).await?;
    let result: TestResult = async {
        fixture.lock(BARRIER).await?;
        fs::write(fixture.dir.join("001_probe.sql"),format!("SELECT pg_catalog.pg_advisory_xact_lock({BARRIER});CREATE TABLE public.probe(id INT)"))?;
        let first=fixture.run();fixture.wait(BARRIER,None).await?;
        let second_pool=make_pool(&fixture.config);let second_dir=fixture.dir.clone();
        let second=tokio::spawn(async move{migrate(&second_pool,second_dir).await});fixture.wait(CLI_KEY,None).await?;
        fixture.unlock(BARRIER).await?;
        timeout(Duration::from_secs(8),first).await???;
        timeout(Duration::from_secs(8),second).await???;
        ensure(fixture.business_exists().await?,"business step absent")?;
        let row=fixture.control.query_one(&format!("SELECT pg_catalog.count(*)::bigint FROM public.{LEDGER}"),&[]).await?;
        ensure(row.try_get::<_,i64>(0)?==1,"competing runs did not record exactly one filename")?;
        Ok(())
    }.await;
    fixture.cleanup().await?;
    result
}

#[tokio::test]
async fn canceled_future_discards_pool_session_and_eventually_unlocks() -> TestResult {
    let Some(url) = admin_url() else {
        return Ok(());
    };
    for during_history in [false, true] {
        let fixture = Fixture::new(&url).await?;
        let result: TestResult = async {
            let key=if during_history {BARRIER}else{CLI_KEY};fixture.lock(key).await?;
            fs::write(fixture.dir.join("001_probe.sql"),"CREATE TABLE public.probe(id INT)")?;
            if during_history {
                fixture.control.batch_execute(&format!("CREATE TABLE public.{LEDGER}(name TEXT NOT NULL PRIMARY KEY,applied_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now());CREATE FUNCTION public.park_record() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_catalog.pg_advisory_xact_lock({BARRIER}); RETURN NEW; END $$;CREATE TRIGGER park_record BEFORE INSERT ON public.{LEDGER} FOR EACH ROW EXECUTE FUNCTION public.park_record()" )).await?;
            }
            let checked_out=fixture.pool.get().await?;
            let old:i32=checked_out.client().query_one("SELECT pg_catalog.pg_backend_pid()",&[]).await?.try_get(0)?;
            drop(checked_out);
            let task=fixture.run();fixture.wait(key,Some(old)).await?;
            if during_history {
                let held:bool=fixture.control.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND ((classid::bigint<<32)|objid::bigint)=$2)",&[&old,&CLI_KEY]).await?.try_get(0)?;
                ensure(held,"runner did not hold actual CLI advisory lock")?;
            }
            task.abort();ensure(match task.await { Err(error)=>error.is_cancelled(),Ok(_)=>false },"future was not canceled")?;
            let replacement=timeout(Duration::from_secs(5),fixture.pool.get()).await??;
            let row=replacement.client().query_one("SELECT pg_catalog.pg_backend_pid(),(SELECT pg_catalog.count(*)::bigint FROM pg_catalog.pg_locks WHERE locktype='advisory' AND pid=pg_catalog.pg_backend_pid())",&[]).await?;
            ensure(row.try_get::<_,i32>(0)?!=old,"canceled session was recycled")?;
            ensure(row.try_get::<_,i64>(1)?==0,"replacement session carried advisory locks")?;
            drop(replacement);
            fixture.unlock(key).await?;
            // Client drop need not interrupt an in-flight PG statement. Only
            // after releasing the controlled barrier require eventual closure.
            timeout(Duration::from_secs(8),async {
                while fixture.control.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1)",&[&old]).await?.try_get::<_,bool>(0)? {sleep(Duration::from_millis(10)).await;}
                Ok::<_,Box<dyn Error+Send+Sync>>(())
            }).await??;
            ensure(!fixture.business_exists().await?,"controlled canceled pre-commit step persisted")?;
            if during_history {
                let row=fixture.control.query_one(&format!("SELECT pg_catalog.count(*)::bigint FROM public.{LEDGER}"),&[]).await?;
                ensure(row.try_get::<_,i64>(0)?==0,"controlled canceled pre-commit history persisted")?;
            }
            Ok(())
        }.await;
        fixture.cleanup().await?;
        result?;
    }
    Ok(())
}

#[tokio::test]
async fn foreign_and_native_type_identity_refuse_before_effects() -> TestResult {
    let Some(url) = admin_url() else {
        return Ok(());
    };
    for case in ["cli", "rust", "domain"] {
        let fixture = Fixture::new(&url).await?;
        let result: TestResult = async {
            fs::write(fixture.dir.join("001_probe.sql"),"CREATE TABLE public.probe(id INT)")?;
            let sql=match case {
                "cli"=>"CREATE TABLE public._neutron_migrations(version TEXT PRIMARY KEY)".to_string(),
                "rust"=>format!("CREATE TABLE public.{FOREIGN}(name TEXT NOT NULL PRIMARY KEY,applied_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now())"),
                _=>format!("CREATE DOMAIN public.text AS pg_catalog.text;CREATE TABLE public.{LEDGER}(name public.text NOT NULL PRIMARY KEY,applied_at pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.now());ALTER DATABASE {} SET search_path TO public,pg_catalog",fixture.name),
            };
            fixture.control.batch_execute(&sql).await?;
            ensure(migrate(&fixture.pool,&fixture.dir).await.is_err(),"foreign/domain history was admitted")?;
            ensure(!fixture.business_exists().await?,"refused admission ran business SQL")?;
            if case!="domain" {
                let own:bool=fixture.control.query_one("SELECT pg_catalog.to_regclass($1) IS NOT NULL",&[&format!("public.{LEDGER}")]).await?.try_get(0)?;
                ensure(!own,"foreign admission created own metadata")?;
            }
            Ok(())
        }.await;
        fixture.cleanup().await?;
        result?;
    }
    Ok(())
}
