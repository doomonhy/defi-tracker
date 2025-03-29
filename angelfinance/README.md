## Files settings

* wallet.addr - target address
* angels-wallet.addr - addresses for the ADA rewards (Angel Finance)

## How to use

0. Fully synced cardano-node and dbsync (cexplorer psql)
1. Run find_txs.py to scrape all the txs of the wallet address (this may take a while depending on the amount of txs the address has, after you run for the first time it'll only look for txs after the latest one, so should be quicker)
2. Run rewards.py to scrape all ADA airdropped amounts, it will also gather the date and time of each tx (airdrop happens every 15th day of the month at random time)
3. transactions.csv and rewards.csv will be created containing all the relevant data
