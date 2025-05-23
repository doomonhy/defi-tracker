import csv
import psycopg2
import re

# Read transaction hashes from CSV file
def read_transaction_hashes(file_path):
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        hashes = [row[0] for row in csv_reader if row]
        return [f"\\x{tx_hash}" if not tx_hash.startswith("\\x") and re.match(r'^[a-fA-F0-9]+$', tx_hash) else tx_hash for tx_hash in hashes]

# Read the address from a file
def read_address(file_path):
    with open(file_path, mode='r') as file:
        content = file.read().strip()
        # If it's the ada-rewards file, return a list of addresses
        if 'ada-rewards.addr' in file_path:
            return [addr.strip() for addr in content.split('\n') if addr.strip()]
        # For other files, return single address as before
        return content

# Connect to the PostgreSQL database and execute the query
def check_addresses(transaction_hashes, target_addresses, wallet_address):
    connection = psycopg2.connect(
        dbname='cexplorer',
        user='user',
        password='password',
        host='localhost',
        port='5432'
    )
    cursor = connection.cursor()

    query = """
    SELECT DISTINCT ON (tx.hash)
        tx.hash AS txid,
        to_char(block.time, 'YYYY-MM-DD') AS tx_date, 
        tx_out.address AS output_address
    FROM tx_out
    INNER JOIN tx ON tx_out.tx_id = tx.id
    INNER JOIN block ON tx.block_id = block.id
    WHERE tx.hash = %s
    AND tx_out.address = ANY(%s);
    """

    matches = {'total': 0, 'indy': [], 'ada': []}
    processed_txs = set()  # Keep track of processed transactions

    for tx_hash in transaction_hashes:
        # Create a list of all addresses to check (indy address + all ada addresses)
        all_addresses = [target_addresses['indy']] + target_addresses['ada']
        cursor.execute(query, (tx_hash, all_addresses))
        results = cursor.fetchall()

        for row in results:
            txid, tx_date, output_address = row
            
            # Skip if we've already processed this transaction
            if txid in processed_txs:
                continue
                
            processed_txs.add(txid)
            matches['total'] += 1

            if output_address == target_addresses['indy']:
                indy_amount = get_wallet_amount(cursor, txid, wallet_address, 'indy')
                matches['indy'].append((txid, tx_date, 0, indy_amount))
            elif output_address in target_addresses['ada']:
                ada_amount = get_wallet_amount(cursor, txid, wallet_address, 'ada')
                matches['ada'].append((txid, tx_date, ada_amount, 0))

    cursor.close()
    connection.close()

    return matches

def get_wallet_amount(cursor, txid, wallet_address, asset_type):
    if asset_type == 'ada':
        query = """
        WITH inputs AS (
            SELECT COALESCE(SUM(tx_out.value), 0) as input_sum
            FROM tx
            JOIN tx_in ON tx_in.tx_in_id = tx.id
            JOIN tx_out ON tx_in.tx_out_id = tx_out.tx_id 
                AND tx_in.tx_out_index = tx_out.index
            WHERE tx.hash = %s AND tx_out.address = %s
        ),
        outputs AS (
            SELECT COALESCE(SUM(tx_out.value), 0) as output_sum
            FROM tx
            JOIN tx_out ON tx_out.tx_id = tx.id
            WHERE tx.hash = %s AND tx_out.address = %s
        )
        SELECT (output_sum - input_sum) / 1000000.0 as ada_amount
        FROM inputs, outputs;
        """
        cursor.execute(query, (txid, wallet_address, txid, wallet_address))
        result = cursor.fetchone()
        return float(result[0]) if result and result[0] is not None else 0
    else:  # indy
        query = """
        SELECT 
            ma_tx_out.quantity / 1000000.0 AS indy_amount,
            tx_out.address,
            ma.policy,
            ma.name
        FROM tx_out
        JOIN ma_tx_out ON ma_tx_out.tx_out_id = tx_out.id
        JOIN ma_tx_mint ON ma_tx_out.ident = ma_tx_mint.id
        JOIN multi_asset ma ON ma_tx_mint.ident = ma.id
        WHERE tx_out.tx_id = (SELECT id FROM tx WHERE hash = %s)
        AND tx_out.address = %s
        ORDER BY ma_tx_out.quantity DESC
        LIMIT 1;
        """
    
    cursor.execute(query, (txid, wallet_address))
    if asset_type == 'ada':
        result = cursor.fetchone()
        return float(result[0]) if result and result[0] is not None else 0
    else:
        result = cursor.fetchone()
        if result and result[0] is not None:
            indy_amount, addr, policy, name = result
            return float(indy_amount)
        return 0

def save_to_csv(matches, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['txid', 'tx_date', 'ada_amount', 'indy_amount', 'tx_type'])
        for match in matches['indy']:
            writer.writerow([match[0].hex(), match[1], match[2], match[3], 'INDY rewards withdraw'])  # Empty tx_type for INDY transactions
        for match in matches['ada']:
            writer.writerow([match[0].hex(), match[1], match[2], match[3], tx_type(match[2])])  # Add tx_type for ADA transactions

def tx_type(ada_value):
    if ada_value < 0:
        return "Governance vote"
    else:   
        return "ADA Rewards withdraw"

def main():
    tx_hashes_file = 'transactions.csv'
    indy_address_file = 'indy-rewards.addr'
    ada_address_file = 'ada-rewards.addr'
    wallet_address_file = 'wallet.addr'

    transaction_hashes = read_transaction_hashes(tx_hashes_file)
    indy_address = read_address(indy_address_file)
    ada_addresses = read_address(ada_address_file)  # Now returns a list
    wallet_address = read_address(wallet_address_file)
    target_addresses = {'indy': indy_address, 'ada': ada_addresses}  # ada now contains a list

    matches = check_addresses(transaction_hashes, target_addresses, wallet_address)

    print(f"Target wallet: {wallet_address}")
    print(f"Total TxIds processed: {len(transaction_hashes)}")
    print(f"Total matches found: {matches['total']}")
    print(f"Matches for INDY reward address ({indy_address}): {len(matches['indy'])}")
    print(f"Matches for ADA reward addresses ({', '.join(ada_addresses)}): {len(matches['ada'])}")

    if matches['indy']:
        print("\nTransaction IDs matching INDY address with amounts:")
        for txid, tx_date, ada_amount, indy_amount in matches['indy']:
            print(f"{txid.hex()} | {tx_date} | {indy_amount:>10.6f} INDY")

    if matches['ada']:
        print("\nTransaction IDs matching ADA addresses with amounts:")
        for txid, tx_date, ada_amount, indy_amount in matches['ada']:
            print(f"{txid.hex()} | {tx_date} | {ada_amount:>10.6f} ADA | {tx_type(ada_amount)}")

    save_to_csv(matches, 'rewards.csv')
    print("\nResults have been saved to rewards.csv")

if __name__ == "__main__":
    main()
