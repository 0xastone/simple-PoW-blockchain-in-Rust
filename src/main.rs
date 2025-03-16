use sha2::{Sha256, Digest};
use chrono::Utc;
use std::fmt;
use actix_web::{web, App, HttpServer, Responder, HttpResponse};
use serde::{Serialize, Deserialize};
use std::env;
use std::sync::Mutex;
use reqwest::Client;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Transaction {
    #[serde(default)] // to allow empty transaction id on POST
    id: String, 
    sender: String,
    receiver: String,
    amount: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Block {
    index: u32,
    timestamp: i64,
    transactions: Vec<Transaction>,
    prev_hash: String,
    hash: String,
    nonce: u64,
}

impl Block {
    // Constructor for a new Block
    fn new(index: u32, transactions: Vec<Transaction>, prev_hash: String) -> Block {
        let timestamp = Utc::now().timestamp();
        let mut block = Block {
            index,
            timestamp,
            transactions,
            prev_hash,
            hash: String::new(),
            nonce: 0,
        };
        block.mine_block(4); // Mine the block with a difficulty of 4
        block
    }

    // Calculate the SHA-256 hash of the block
    fn calculate_hash(&self) -> String {
        // Serialize transactions into a string for hashing
        let txs_str: String = self.transactions
            .iter()
            .map(|tx| format!("{}->{}:{}", tx.sender, tx.receiver, tx.amount))
            .collect::<Vec<String>>()
            .join(",");
        let input = format!(
            "{}{}{}{}{}",
            self.index,
            self.timestamp,
            txs_str,
            self.prev_hash,
            self.nonce
        );
        let mut hasher = Sha256::new();
        hasher.update(input);
        let result = hasher.finalize();
        format!("{:x}", result) // Convert to hexadecimal string
    }

    fn mine_block(&mut self, difficulty: usize) {
        let target = "0".repeat(difficulty);
        loop {
            self.hash = self.calculate_hash();
            if self.hash[..difficulty] == target {
                println!("Block mined! Hash: {}", self.hash);
                break;
            }
            self.nonce += 1;
        }
    }
}

// Improve the Display of Blocks
impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let txs_str: String = self.transactions
            .iter()
            .map(|tx| format!("{}->{}:{}", tx.sender, tx.receiver, tx.amount))
            .collect::<Vec<String>>()
            .join(",");
        write!(
            f,
            "Block #{} [Hash: {}, Prev: {}, transactions:{}, Nonce: {}]",
            self.index, self.hash, self.prev_hash, txs_str, self.nonce
        )
    }
}

#[derive(Serialize, Deserialize)]
struct Blockchain {
    chain: Vec<Block>, // Vec<T> is a growable array
    pending_transactions: Vec<Transaction>, // Store Transactions before mining
}

impl Blockchain {
    // Create a new blockchain with a genesis block
    fn new() -> Blockchain {
        let mut blockchain = Blockchain {
            chain: Vec::new(),
            pending_transactions: Vec::new(),
        };
        blockchain.add_genesis_block();
        blockchain
    }

    fn add_genesis_block(&mut self) {
        let genesis_txs = vec![Transaction {
            id: "genesis".to_string(),
            sender: "Genesis".to_string(),
            receiver: "Genesis".to_string(),
            amount: 0.0,
        }];
        let genesis = Block::new(0, genesis_txs, "0".to_string());
        self.chain.push(genesis);
    }

    // Get the latest block
    fn get_latest_block(&self) -> &Block {
        self.chain.last().unwrap()
    }

    // Add a transaction to pending list
    fn add_transaction(&mut self, tx: Transaction) {
        self.pending_transactions.push(tx);
    }

    // Mine a new Block with pending transactions
    fn mine_block(&mut self) {
        // Split the borrows to avoid a borrow checker issue with &mut self.pending_transactions
        let previous_block = self.get_latest_block();
        let prev_index = previous_block.index;
        let prev_hash = previous_block.hash.clone(); // Clone the hash to own it

        // Now mutable borrow is safe because previous_block is no longer borrowed
        let transactions = std::mem::take(&mut self.pending_transactions);
        let new_block = Block::new(prev_index + 1, transactions, prev_hash);
        self.chain.push(new_block);
    }

    //Validate a chain
    fn is_valid_chain(&self, chain: &[Block]) -> bool {
        if chain.is_empty() || chain[0].prev_hash != "0" {
            return false; //Check genesis block
        }
        for i in 1..chain.len() {
            let current = &chain[i];
            let previous = &chain[i - 1];
            if current.prev_hash != previous.hash || current.hash != current.calculate_hash() {
                return false; // Check hash continuity and integrity
            }
        }
        true
    }

    // Replace the chain with a new one if it is longer and valid
    fn replace_chain(&mut self, new_chain: Vec<Block>) {
        if new_chain.len() > self.chain.len() && self.is_valid_chain(&new_chain) {
            println!("Replacing chain with longer valid chain");
            // Collect all transaction IDs from the new chain
            let confirmed_tx_ids: Vec<String> = new_chain
                .iter()
                .flat_map(|block| block.transactions.iter().map(|tx| tx.id.clone()))
                .collect();
            // Remove pending transactions that are now confirmed
            self.pending_transactions.retain(|tx| !confirmed_tx_ids.contains(&tx.id));
            self.chain = new_chain;
        }
    }
}

// Share Blockchain across requests
struct AppState {
    blockchain: Mutex<Blockchain>, // Mutex for thread-safe access
    peers: Vec<String>, // List of peer addresses
}

async fn index() -> impl Responder {
    HttpResponse::Ok().body("Blockchain Node Running")
}

async fn create_transaction(tx: web::Json<Transaction>, state: web::Data<AppState>, client: web::Data<Client>) -> impl Responder {
    let mut blockchain = state.blockchain.lock().unwrap();
    let mut tx = tx.into_inner();
    if tx.id.is_empty() {
        tx.id = Utc::now().timestamp_nanos().to_string(); // Assign unique ID
    }
    blockchain.add_transaction(tx.clone()); // Add locally first

    // Spawn a task to broadcast asynchronously
    let peers = state.peers.clone();
    let client = client.clone();
    tokio::spawn(async move {
        for peer in &peers {
            let url = format!("http://{}/broadcast", peer);
            match client.post(&url).json(&tx).send().await {
                Ok(_) => println!("Broadcasted transaction to {}", peer),
                Err(e) => println!("Failed to broadcast to {}: {}", peer, e),
            }
        }
    });

    HttpResponse::Ok().body("Transaction added and broadcasted")
}


async fn broadcast_transaction(tx: web::Json<Transaction>, state: web::Data<AppState>) -> impl Responder {
    println!("Received broadcast: {:?}", tx); // Log the incoming transaction
    let mut blockchain = state.blockchain.lock().unwrap();
    blockchain.add_transaction(tx.into_inner());
    HttpResponse::Ok().body("Transaction received from broadcast")
}

async fn mine(state: web::Data<AppState>) -> impl Responder {
    let mut blockchain = state.blockchain.lock().unwrap();
    blockchain.mine_block();
    HttpResponse::Ok().body("New block mined")
}

async fn get_chain(state: web::Data<AppState>) -> impl Responder {
    let blockchain = state.blockchain.lock().unwrap();
    HttpResponse::Ok().json(&*blockchain) // Dereference the MutexGuard for serialization
}


// Fetch peer chains asynchronously without holding the lock
async fn fetch_peer_chains(peers: &[String], client: &Client) -> Vec<Blockchain>{
    let mut chains = Vec::new();
    for peer in peers {
        let url = format!("http://{}/chain", peer);
        match client.get(&url).send().await {
            Ok(response) => {
                if let Ok(peer_chain) = response.json::<Blockchain>().await {
                    chains.push(peer_chain);
                }
            }
            Err(e) => {
                println!("Failed to fetch chain from {}: {}", peer, e);
            }
        }
    }
    chains
}

// Background task to periodically sync
async fn sync_task(state: web::Data<AppState>, peers: Vec<String>) {
    let client = Client::new();
    loop {
        // Step 1: Fetch peer chains without the lock
        let peer_chains = fetch_peer_chains(&peers, &client).await;

        // Step 2: Lock the blockchain only for updating
        {
            let mut blockchain = state.blockchain.lock().unwrap();
            for peer_chain in peer_chains {
                blockchain.replace_chain(peer_chain.chain);
            }
        } // Lock is released here

        // Step 3: Sleep until the next sync
        sleep(Duration::from_secs(10)).await;
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let port = args.get(1).unwrap_or(&"8080".to_string()).parse::<u16>().unwrap();
    let bind_addr = format!("127.0.0.1:{}", port);

    let peers = vec![
        "127.0.0.1:8080".to_string(),
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
    ];

    let blockchain = Blockchain::new();
    let app_state = web::Data::new(AppState {
        blockchain: Mutex::new(blockchain),
        peers: peers.clone(),
    });
    let client = web::Data::new(Client::new()); // Shared HTTP client

    // Spawn sync task
    let sync_state = app_state.clone();
    tokio::spawn(sync_task(sync_state, peers));

    println!("Starting node on {}", bind_addr);
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone()) // Share Appstate across handlers
            .app_data(client.clone()) //Share Client
            .route("/", web::get().to(index)) // Dummy route for testing
            .route("/transaction", web::post().to(create_transaction)) // Create a new transaction
            .route("/broadcast", web::post().to(broadcast_transaction)) // Broadcast a transaction
            .route("/mine", web::post().to(mine)) // Mine a new block
            .route("/chain", web::get().to(get_chain)) // Get the full chain
    })
    .bind(bind_addr)?
    .run()
    .await
}