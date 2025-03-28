import psycopg2
import csv
import os

# Database configuration
DB_CONFIG = {
    "dbname": "cexplorer",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": 5432,
}

def read_address_from_file(filename):
    """Reads an address from a file."""
    try:
        with open(filename, 'r') as file:
            return file.read().strip()
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return None

def get_latest_tx_date_from_csv(filename):
    """Get the latest transaction date from existing CSV file."""
    try:
        with open(filename, mode='r', newline='') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            dates = [int(row[1]) for row in reader if row[1].isdigit()]
            return max(dates) if dates else None
        
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"Error reading latest date from CSV: {e}")
        return None

def get_transactions_for_address(address, conn, start_block=None):
    """Fetch all transactions for the given address from the specified block."""
    query = """
    SELECT
        encode(tx.hash, 'hex') AS tx_hash,
        tx.block_id,
        b.time AS block_time
    FROM tx_out
    JOIN tx ON tx.id = tx_out.tx_id
    JOIN block b ON b.id = tx.block_id
    WHERE tx_out.address = %s
    {}
    ORDER BY b.time DESC;
    """
    
    if start_block:
        query = query.format("AND tx.block_id > %s")
        params = (address, start_block)
    else:
        query = query.format("")
        params = (address,)
        
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    except Exception as e:
        print(f"Error fetching transactions for {address}: {e}")
        return []

def save_transactions_to_csv(transactions, filename, mode='w'):
    """Save the list of transactions to a CSV file."""
    try:
        with open(filename, mode=mode, newline='') as file:
            writer = csv.writer(file)
            if mode == 'w':  # Only write header for new files
                writer.writerow(['tx_hash', 'block_id', 'timestamp'])
            writer.writerows(transactions)
        print(f"Transactions saved to {filename}")
    except Exception as e:
        print(f"Error saving transactions to CSV: {e}")

def main():
    csv_filename = "transactions.csv"
    
    # Read address from file
    address = read_address_from_file("wallet.addr")
    
    if not address:
        print("Error: Unable to read address from wallet.addr file.")
        return

    # Connect to the database
    try:
        print("-- Connecting to the database...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("-> Connected to the database successfully.\n")
        print("-- This may take a while depending on how many txs the wallet has.\n-> Looking for txs...\n")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return
    
    try:
        # Get the latest transaction date from existing CSV
        latest_block = get_latest_tx_date_from_csv(csv_filename)
        
        # Fetch transactions for the given address
        transactions = get_transactions_for_address(address, conn, latest_block)
        print(f"Found {len(transactions)} new transactions for {address}.\n")

        if transactions:
            # Determine write mode based on whether we're updating or creating
            mode = 'a' if latest_block else 'w'
            save_transactions_to_csv(transactions, csv_filename, mode)

            # Print sample of transactions found
            for tx_hash, block_id, timestamp in transactions[:5]:  # Show first 5 transactions
                print("Printing the latest txs...")
                print(f"TxId: {tx_hash} Block: {block_id} Date/time: {timestamp}")
            if len(transactions) > 5:
                print(f"... and {len(transactions) - 5} more")
        else:
            # If no new transactions, read and display the latest from CSV
            try:
                with open(csv_filename, mode='r', newline='') as file:
                    reader = csv.reader(file)
                    next(reader)  # Skip header
                    rows = list(reader)  # Convert to list to make sure it's not empty
                    if not rows:
                        print("No transaction history found.")
                        return
                        
                    latest_tx = rows[0]
                    max_block = int(rows[0][1])  # Start with first row's block_id
                    
                    for row in rows[1:]:  # Start from second row
                        block_id = int(row[1])
                        if block_id > max_block:
                            max_block = block_id
                            latest_tx = row
                            
                    print(f"No new transactions. Latest transaction:")
                    print(f"TX: {latest_tx[0]}")
                    print(f"Block: {latest_tx[1]}")
                    print(f"Time: {latest_tx[2]}")
            except FileNotFoundError:
                print("No transaction history found.")

    except Exception as e:
        print(f"An error occurred during processing: {e}")

    finally:
        # Close the connection to the database
        try:
            if conn:
                conn.close()
                print("\n-- Database connection closed.")
        except Exception as e:
            print(f"Error closing the database connection: {e}")

if __name__ == "__main__":
    main()

