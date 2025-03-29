## Files settings

* wallet.addr - target address
* stuff-wallet.addr - address for the STUFF/BOOK rewards (Stuff.io/Book.io)

## How to use

0. Fully synced cardano-node and dbsync (cexplorer psql)
1. Run find_txs.py to scrape all the txs of the wallet address (this may take a while depending on the amount of txs the address has, after you run for the first time it'll only look for txs after the latest one, so should be quicker)
2. Run rewards.py to scrape all STUFF/BOOK airdropped amounts, it will also gather the minADAUTxO amount that was recivied
3. transactions.csv and rewards.csv will be created containing all the relevant data
