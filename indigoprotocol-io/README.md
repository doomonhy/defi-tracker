1. Run find_txs.py to scrape all the txs of the wallet address (this may take a while depending on the amount of txs)
2. Run rewards.py to scrape all ADA and INDY rewards withdraw amounts, the script will also gather the Governance txs (ADA value will be negative) for redundancy (you need to vote on Governance to earn INDY rewards)
3. transactions.csv and rewards.csv will be created containing all the relevant data
