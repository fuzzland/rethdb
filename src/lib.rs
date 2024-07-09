use reth_primitives::StorageKey;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use alloy_chains::{Chain, NamedChain};
use dashmap::{DashMap, Entry};
use eyre::{bail, Context};
use reth_db::{DatabaseEnv, open_db_read_only};
use reth_provider::{BlockNumReader, DatabaseProviderRO, ProviderFactory, StateProviderBox};
use once_cell::sync::Lazy;
use reth_chainspec::ChainSpecBuilder;
use reth_db::mdbx::DatabaseArguments;
use reth_db_api::models::ClientVersion;
use reth_provider::providers::StaticFileProvider;
use revm::{Database, DatabaseRef, interpreter};
use revm::primitives::{AccountInfo, Address, B256, Bytecode, KECCAK_EMPTY, U256};

type DBFactory = ProviderFactory<Arc<DatabaseEnv>>;
type Lock = Arc<Mutex<()>>;

static DB_LOCK: Lazy<DashMap<String, (DBFactory, Lock)>> = Lazy::new(|| Default::default());

pub struct RethDB {
    provider: StateProviderBox,

    measure_rpc_time: bool,
    cumulative_rpc_time: AtomicU64,
}

impl RethDB {
    pub fn new(chain: Chain, db_path: &str, block: Option<u64>) -> eyre::Result<Self> {
        let (database, lock) = match DB_LOCK.entry(db_path.to_string()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let db_path = Path::new(db_path);

                let db_env = open_db_read_only(
                    db_path.join("db").as_path(),
                    DatabaseArguments::new(ClientVersion::default()),
                )
                    .context("fail to open db env")?;
                let db_env = Arc::new(db_env);

                let static_file_provider = StaticFileProvider::read_only(db_path.join("static_files"))
                    .context("fail to open static file provider")?;

                let spec = match chain.named() {
                    Some(NamedChain::Mainnet) => ChainSpecBuilder::mainnet().build(),
                    _ => panic!("unsupported chain"),
                };
                let spec = Arc::new(spec);

                let database = ProviderFactory::new(db_env.clone(), spec.clone(), static_file_provider);

                let lock = Lock::default();
                entry.insert((database.clone(), lock.clone()));

                (database, lock)
            }
        };

        // database.provider will initiate a new transaction to the DB, and it's not
        // allowed to be shared between threads. So we need a lock to make sure we open
        // the transaction sequentially.
        let _guard = lock.lock().unwrap();

        let db_provider = database.provider().context("fail to get provider")?;

        let state_provider = if let Some(block) = block {
            Self::get_state_provider_with_retry(db_provider, block, 500, Duration::from_millis(10))?
        } else {
            database.latest().context("fail to get latest state provider")?
        };

        Ok(Self {
            provider: state_provider,

            measure_rpc_time: false,
            cumulative_rpc_time: AtomicU64::new(0),
        })
    }

    fn get_state_provider_with_retry(
        db_provider: DatabaseProviderRO<Arc<DatabaseEnv>>,
        block: u64,
        mut max_retries: usize,
        interval: Duration,
    ) -> eyre::Result<StateProviderBox> {
        max_retries += 1;

        while max_retries > 0 {
            let best_block = db_provider
                .best_block_number()
                .context("fail to get best block number")?;

            if block > best_block {
                std::thread::sleep(interval);
                max_retries -= 1;
                continue;
            }

            let s_provider = db_provider
                .state_provider_by_block_number(block)
                .context("fail to state provider by block")?;

            return Ok(s_provider);
        }

        bail!("timeout waiting for latest block")
    }

    fn request<F, R, E>(&self, f: F) -> Result<R, E>
        where
            F: FnOnce(&StateProviderBox) -> Result<R, E>,
    {
        if self.measure_rpc_time {
            let start = std::time::Instant::now();
            let result = f(&self.provider);

            let elapsed = start.elapsed().as_nanos() as u64;
            self.cumulative_rpc_time.fetch_add(elapsed, Ordering::Relaxed);

            result
        } else {
            let result = f(&self.provider);

            result
        }
    }

    pub fn set_measure_rpc_time(&mut self, enable: bool) {
        self.measure_rpc_time = enable;
    }

    pub fn get_rpc_time(&self) -> Duration {
        Duration::from_nanos(self.cumulative_rpc_time.load(Ordering::Relaxed))
    }

    pub fn reset_rpc_time(&mut self) {
        self.cumulative_rpc_time.store(0, Ordering::Relaxed);
    }
}

impl Database for RethDB {
    type Error = eyre::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Self::basic_ref(self, address)
    }

    fn code_by_hash(&mut self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        panic!("This should not be called, as the code is already loaded");
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        Self::storage_ref(self, address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        Self::block_hash_ref(self, number)
    }
}

impl DatabaseRef for RethDB {
    type Error = eyre::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let account = self
            .request(|provider| provider.basic_account(address))?
            .unwrap_or_default();
        let code = self
            .request(|provider| provider.account_code(address))?
            .unwrap_or_default();

        let code = interpreter::analysis::to_analysed(Bytecode::new_raw(code.original_bytes()));

        Ok(Some(AccountInfo::new(
            account.balance,
            account.nonce,
            account.bytecode_hash.unwrap_or_default(),
            code,
        )))
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        panic!("This should not be called, as the code is already loaded");
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let value = self.request(|provider| provider.storage(address, StorageKey::from(index)))?;

        Ok(value.unwrap_or_default())
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let blockhash = self.request(|provider| provider.block_hash(number))?;

        if let Some(hash) = blockhash {
            Ok(B256::new(hash.0))
        } else {
            Ok(KECCAK_EMPTY)
        }
    }
}
