#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Block Data Synchronization Script
Extract block data from local JSON files and store into SQLite database
Support incremental updates and automatic periodic checking for new files
"""

import os
import json
import sqlite3
import time
import logging
from datetime import datetime
from pathlib import Path

class BlockSyncer:
    def __init__(self, data_dir="/opt/xenblocks/data", db_path="/opt/xenblocks/db/blocks.db", check_interval=10):
        self.data_dir = data_dir
        self.db_path = db_path
        self.check_interval = check_interval  # Interval to check for new files (seconds)
        self.setup_logging()
        self.setup_database()
        
    def setup_logging(self):
        """Setup logging configuration"""
        # Configure main logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('sync_blocks.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        # Configure dedicated logger for error pages
        self.error_logger = logging.getLogger('error_pages')
        error_handler = logging.FileHandler('error_pages.log', encoding='utf-8')
        error_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.error_logger.addHandler(error_handler)
        self.error_logger.setLevel(logging.ERROR)
        
    def setup_database(self):
        """Initialize SQLite database and table structure"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("PRAGMA journal_mode=WAL;")

            # Create blocks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blocks (
                    block_id INTEGER PRIMARY KEY,
                    account TEXT NOT NULL,
                    date TEXT NOT NULL,
                    key TEXT NOT NULL
                )
            ''')
            
            # Create indexes to improve query performance
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_account ON blocks(account)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_date ON blocks(date)
            ''')
            
            conn.commit()
            conn.close()
            logging.info(f"Database initialized successfully: {self.db_path}")
            
        except Exception as e:
            logging.error(f"Database initialization failed: {e}")
            raise
    
    def get_max_block_id(self):
        """Get the maximum block_id from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(block_id) FROM blocks")
            result = cursor.fetchone()
            conn.close()
            
            max_id = result[0] if result[0] is not None else 0
            logging.info(f"Current maximum block_id in database: {max_id}")
            return max_id
        except Exception as e:
            logging.error(f"Failed to query maximum block_id: {e}")
            return 0
    
    def get_existing_block_ids(self, start_id=None, end_id=None):
        """Get set of existing block_ids within specified range"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if start_id is not None and end_id is not None:
                cursor.execute("SELECT block_id FROM blocks WHERE block_id BETWEEN ? AND ?", 
                             (start_id, end_id))
            else:
                cursor.execute("SELECT block_id FROM blocks")
            
            existing_ids = set(row[0] for row in cursor.fetchall())
            conn.close()
            return existing_ids
        except Exception as e:
            logging.error(f"Failed to query existing block_ids: {e}")
            return set()
    
    def load_json_file(self, file_path):
        """Load and parse JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                raise ValueError("JSON file content is not a list")
            
            return data
        except FileNotFoundError:
            return None  # File does not exist
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing error: {e}")
        except Exception as e:
            raise ValueError(f"File reading error: {e}")
    
    def validate_block_data(self, block_data):
        """Validate the integrity of block data"""
        required_fields = ['account', 'block_id', 'date', 'key']
        
        for field in required_fields:
            if field not in block_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate data types
        if not isinstance(block_data['block_id'], int):
            raise ValueError(f"block_id must be integer: {block_data['block_id']}")
        
        if not isinstance(block_data['account'], str):
            raise ValueError(f"account must be string: {block_data['account']}")
        
        if not isinstance(block_data['date'], str):
            raise ValueError(f"date must be string: {block_data['date']}")
            
        if not isinstance(block_data['key'], str):
            raise ValueError(f"key must be string: {block_data['key']}")
        
        return True
    
    def insert_blocks(self, blocks_data):
        """Batch insert block data into database"""
        if not blocks_data:
            return 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Prepare insert data
            insert_data = []
            for block in blocks_data:
                insert_data.append((
                    block['block_id'],
                    block['account'],
                    block['date'],
                    block['key']
                ))
            
            # Batch insert
            cursor.executemany('''
                INSERT OR IGNORE INTO blocks (block_id, account, date, key)
                VALUES (?, ?, ?, ?)
            ''', insert_data)
            
            inserted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            return inserted_count
        except Exception as e:
            logging.error(f"Failed to insert data: {e}")
            return 0
    
    def process_page_file(self, page_num):
        """Process a single page file"""
        file_path = os.path.join(self.data_dir, f"page_{page_num}.json")
        
        try:
            # Load JSON file
            blocks_data = self.load_json_file(file_path)
            if blocks_data is None:
                return None  # File does not exist
            
            valid_blocks = []
            invalid_count = 0
            
            # Validate each block data
            for i, block in enumerate(blocks_data):
                try:
                    self.validate_block_data(block)
                    valid_blocks.append(block)
                except ValueError as e:
                    invalid_count += 1
                    self.error_logger.error(f"Page {page_num} record {i+1} invalid: {e}")
            
            if not valid_blocks:
                self.error_logger.error(f"Page {page_num} has no valid block data")
                return 0
            
            # Check which blocks need to be inserted (incremental update)
            block_ids_in_page = [block['block_id'] for block in valid_blocks]
            min_id = min(block_ids_in_page)
            max_id = max(block_ids_in_page)
            
            existing_ids = self.get_existing_block_ids(min_id, max_id)
            
            # Filter blocks that need to be inserted
            blocks_to_insert = [
                block for block in valid_blocks 
                if block['block_id'] not in existing_ids
            ]
            
            if not blocks_to_insert:
                logging.info(f"Page {page_num}: All {len(valid_blocks)} records already exist, skipped")
                return 0
            
            # Insert new blocks
            inserted_count = self.insert_blocks(blocks_to_insert)
            
            if invalid_count > 0:
                logging.warning(f"Page {page_num}: Inserted {inserted_count} records, "
                              f"skipped {len(valid_blocks) - inserted_count} existing records, "
                              f"{invalid_count} invalid records")
            else:
                logging.info(f"Page {page_num}: Inserted {inserted_count} records, "
                           f"skipped {len(valid_blocks) - inserted_count} existing records")
            
            return inserted_count
            
        except ValueError as e:
            self.error_logger.error(f"Page {page_num} processing failed: {e}")
            return 0
        except Exception as e:
            self.error_logger.error(f"Page {page_num} processing exception: {e}")
            return 0
    
    def sync_all_pages(self):
        """Synchronize all page files"""
        logging.info("Starting block data synchronization...")
        
        # Display current database status
        max_block_id = self.get_max_block_id()
        
        page_num = 1
        total_inserted = 0
        consecutive_missing = 0
        max_consecutive_missing = 5  # Threshold for consecutive missing pages
        
        while True:
            result = self.process_page_file(page_num)
            
            if result is None:  # File does not exist
                consecutive_missing += 1
                if consecutive_missing >= max_consecutive_missing:
                    logging.info(f"Consecutive {consecutive_missing} page files missing, waiting for new files...")
                    # Wait for a period then retry current page
                    time.sleep(self.check_interval)
                    consecutive_missing = 0  # Reset counter to continue trying current page
                    continue
                else:
                    logging.debug(f"Page {page_num} does not exist, checking next page")
                    page_num += 1
                    continue
            else:
                consecutive_missing = 0  # Reset consecutive missing counter
                if result > 0:
                    total_inserted += result
                page_num += 1
                
                # Output progress every 100 pages
                if page_num % 100 == 0:
                    current_max = self.get_max_block_id()
                    logging.info(f"Processed {page_num} pages, current max block_id: {current_max}")
    
    def run_continuous(self):
        """Continuous running mode, periodically check for new files"""
        logging.info(f"Starting continuous sync mode, checking for new files every {self.check_interval} seconds")
        
        try:
            self.sync_all_pages()
        except KeyboardInterrupt:
            logging.info("Received interrupt signal, stopping...")
        except Exception as e:
            logging.error(f"Exception occurred during sync process: {e}")
            raise

def main():
    """Main function"""
    # Configuration parameters
    DATA_DIR = "/opt/xenblocks/data"
    DB_PATH = "/opt/xenblocks/db/blocks.db"
    CHECK_INTERVAL = 10  # Check interval (seconds)
    
    # Ensure data directory exists (create if missing)
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except Exception as e:
        print(f"Error: Failed to create data directory: {e}")
        return
    
    try:
        # Create syncer instance
        syncer = BlockSyncer(
            data_dir=DATA_DIR,
            db_path=DB_PATH,
            check_interval=CHECK_INTERVAL
        )
        
        # Display current status
        print("=" * 50)
        print("Block Data Synchronization Tool")
        print("=" * 50)
        print(f"Data directory: {DATA_DIR}")
        print(f"Database file: {DB_PATH}")
        print(f"Check interval: {CHECK_INTERVAL} seconds")
        print("=" * 50)
        
        # Start synchronization
        syncer.run_continuous()
        
    except Exception as e:
        logging.error(f"Program execution failed: {e}")
        print(f"Program execution failed: {e}")

if __name__ == "__main__":
    main()
