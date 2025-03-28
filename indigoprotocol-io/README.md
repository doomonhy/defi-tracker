## Files settings

* wallet.addr - target address
* ada-rewards.addr - addresses for the ADA rewards (Indigo)
* indy-rewards.addr - address for the INDY rewards (Indigo/Sundae)

## How to use

0. Fully synced cardano-node and dbsync (cexplorer psql)
1. Run find_txs.py to scrape all the txs of the wallet address (this may take a while depending on the amount of txs the address has, after you run for the first time it'll only look for txs after the latest one, so should be quicker)
2. Run rewards.py to scrape all ADA and INDY rewards withdraw amounts, the script will also gather the Governance txs (ADA value will be negative) for redundancy (you need to vote on Governance to earn INDY rewards)
3. transactions.csv and rewards.csv will be created containing all the relevant data
