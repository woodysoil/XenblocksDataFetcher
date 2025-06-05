#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XenBlocks Blockchain Data Fetcher Service
Efficient synchronization service with full/incremental sync modes,
resume functionality, retry mechanism, and continuous monitoring.
"""

import os
import json
import time
import random
import requests
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
import logging
from typing import List, Dict, Any, Optional

# ================================
# Configuration Parameters
# ================================
CONFIG = {
    # Network settings
    'leaderboard_url': 'https://xenblocks.io/v1/leaderboard?limit=0&offset=0',
    'blocks_url_template': 'http://xenblocks.io:4445/getblocks/{}',
    'request_timeout': 30,
    
    # Concurrency settings
    'max_workers': 16,
    'batch_multiplier': 10,  # Batch size = max_workers * batch_multiplier
    'max_retries': 5,
    'retry_delay_min': 3,
    'retry_delay_max': 10,
    
    # File settings
    'data_dir': '/opt/xenblocks/data',
    'records_per_page': 100,
    
    # Service settings
    'incremental_check_interval': 120,    # Check for new blocks every 2 minutes
    'error_retry_interval': 300,          # Wait 5 minutes after errors
    'full_sync_threshold': 1000,          # Trigger full sync if gap > 1000 blocks
    'daily_full_sync_interval': 86400,    # Daily full sync as backup (24 hours)
    
    # Logging settings
    'log_level': logging.INFO,
    'log_file': '/var/log/xenblocks/fetcher.log'
}

# ================================
# Global Control Variables
# ================================
shutdown_event = Event()
print_lock = Lock()
stats_lock = Lock()

# Statistics tracking
stats = {
    'total_pages': 0,
    'completed_pages': 0,
    'skipped_pages': 0,
    'failed_pages': set(),
    'start_time': 0,
    'last_sync_time': 0,
    'sync_count': 0,
    'last_full_sync_time': 0
}

# ================================
# Logging Setup
# ================================
def setup_logging():
    """Configure logging for service operation"""
    os.makedirs(os.path.dirname(CONFIG['log_file']), exist_ok=True)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler for persistent logs
    file_handler = logging.FileHandler(CONFIG['log_file'], encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(CONFIG['log_level'])
    
    # Console handler for systemd journal
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(CONFIG['log_level'])
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(CONFIG['log_level'])
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def safe_print(message: str):
    """Thread-safe logging function"""
    with print_lock:
        logger.info(message)

def update_stats(completed: int = 0, skipped: int = 0, failed_page: Optional[int] = None):
    """Update synchronization statistics"""
    with stats_lock:
        stats['completed_pages'] += completed
        stats['skipped_pages'] += skipped
        if failed_page:
            stats['failed_pages'].add(failed_page)

# ================================
# Network and Data Functions
# ================================
def create_session() -> requests.Session:
    """Create configured HTTP session"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'XenBlocks-Fetcher-Service/2.0',
        'Accept': 'application/json',
        'Connection': 'keep-alive'
    })
    return session

def get_total_blocks() -> int:
    """Retrieve current total block count from API"""
    session = create_session()
    
    for attempt in range(CONFIG['max_retries']):
        if shutdown_event.is_set():
            raise InterruptedError("Service shutdown requested")
            
        try:
            response = session.get(
                CONFIG['leaderboard_url'], 
                timeout=CONFIG['request_timeout']
            )
            response.raise_for_status()
            
            data = response.json()
            total_blocks = data.get('totalBlocks', 0)
            
            if total_blocks <= 0:
                raise ValueError(f"Invalid total block count: {total_blocks}")
            
            return total_blocks
            
        except Exception as e:
            if attempt == 0:
                safe_print(f"Failed to get total blocks: {e}")
            
            if attempt < CONFIG['max_retries'] - 1:
                delay = random.uniform(CONFIG['retry_delay_min'], CONFIG['retry_delay_max'])
                if shutdown_event.wait(delay):
                    raise InterruptedError("Service shutdown requested")
            else:
                raise Exception(f"Unable to get total blocks after {CONFIG['max_retries']} attempts")
    
    return 0

def fetch_page_data(page: int, session: requests.Session) -> List[Dict[Any, Any]]:
    """Fetch data for a specific page"""
    if shutdown_event.is_set():
        raise InterruptedError("Service shutdown requested")
        
    url = CONFIG['blocks_url_template'].format(page)
    response = session.get(url, timeout=CONFIG['request_timeout'])
    response.raise_for_status()
    
    data = response.json()
    
    # Validate data structure
    if not isinstance(data, list):
        raise ValueError(f"Page {page} returned invalid format, expected array")
    
    if len(data) != CONFIG['records_per_page']:
        raise ValueError(f"Page {page} incomplete, expected {CONFIG['records_per_page']}, got {len(data)}")
    
    # Validate required fields
    required_fields = ['account', 'block_id', 'date', 'hash_to_verify', 'key']
    for i, record in enumerate(data):
        for field in required_fields:
            if field not in record:
                raise ValueError(f"Page {page} record {i+1} missing field: {field}")
    
    return data

def save_page_data(page: int, data: List[Dict[Any, Any]]) -> bool:
    """Save page data to JSON file"""
    file_path = os.path.join(CONFIG['data_dir'], f'page_{page}.json')
    
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        safe_print(f"Failed to save page {page}: {e}")
        return False

def page_file_exists(page: int) -> bool:
    """Check if page file exists and is not empty"""
    file_path = os.path.join(CONFIG['data_dir'], f'page_{page}.json')
    return os.path.exists(file_path) and os.path.getsize(file_path) > 0

# ================================
# Page Processing Functions
# ================================
def fetch_single_page(page: int) -> bool:
    """Fetch and save single page with retry logic"""
    if shutdown_event.is_set():
        return False
        
    # Skip if file already exists
    if page_file_exists(page):
        update_stats(skipped=1)
        return True
    
    session = create_session()
    
    for attempt in range(CONFIG['max_retries']):
        if shutdown_event.is_set():
            return False
            
        try:
            data = fetch_page_data(page, session)
            
            if save_page_data(page, data):
                if attempt > 0:  # Only log successful retries
                    safe_print(f"Page {page} fetched successfully (attempt {attempt + 1})")
                update_stats(completed=1)
                return True
            else:
                raise Exception("Failed to save file")
                
        except InterruptedError:
            return False
        except Exception as e:
            if attempt == 0:
                safe_print(f"Page {page} failed: {e}")
            
            if attempt < CONFIG['max_retries'] - 1:
                delay = random.uniform(CONFIG['retry_delay_min'], CONFIG['retry_delay_max'])
                if shutdown_event.wait(delay):
                    return False
    
    update_stats(failed_page=page)
    return False

def process_page_batch(batch_pages: List[int]) -> List[int]:
    """Process a batch of pages concurrently"""
    failed_pages = []
    
    with ThreadPoolExecutor(max_workers=CONFIG['max_workers']) as executor:
        future_to_page = {}
        for page in batch_pages:
            if shutdown_event.is_set():
                break
            future_to_page[executor.submit(fetch_single_page, page)] = page
        
        for future in as_completed(future_to_page):
            if shutdown_event.is_set():
                # Cancel remaining futures
                for f in future_to_page:
                    if not f.done():
                        f.cancel()
                break
                
            page = future_to_page[future]
            try:
                success = future.result()
                if not success:
                    failed_pages.append(page)
            except Exception as e:
                safe_print(f"Thread exception for page {page}: {e}")
                failed_pages.append(page)
    
    return failed_pages

def batch_fetch_pages(page_list: List[int]) -> List[int]:
    """Fetch pages in batches to prevent memory overflow"""
    if shutdown_event.is_set() or not page_list:
        return page_list
        
    failed_pages = []
    batch_size = CONFIG['max_workers'] * CONFIG['batch_multiplier']
    
    for i in range(0, len(page_list), batch_size):
        if shutdown_event.is_set():
            failed_pages.extend(page_list[i:])
            break
            
        batch_pages = page_list[i:i + batch_size]
        current_batch = i // batch_size + 1
        total_batches = (len(page_list) + batch_size - 1) // batch_size
        
        safe_print(f"Processing batch {current_batch}/{total_batches}: pages {batch_pages[0]}-{batch_pages[-1]} ({len(batch_pages)} pages)")
        
        batch_failed = process_page_batch(batch_pages)
        failed_pages.extend(batch_failed)
        
        # Brief pause between batches
        if not shutdown_event.is_set() and i + batch_size < len(page_list):
            time.sleep(0.1)
    
    return failed_pages

# ================================
# Synchronization State Management
# ================================
def get_sync_status() -> Dict[str, Any]:
    """Read synchronization status from file"""
    status_file = os.path.join(CONFIG['data_dir'], 'sync_status.json')
    default_status = {
        'last_synced_blocks': 0,
        'last_update_time': 0,
        'last_full_sync_time': 0,
        'sync_count': 0
    }
    
    try:
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                data = json.load(f)
                return {**default_status, **data}
    except Exception as e:
        safe_print(f"Error reading sync status: {e}")
    
    return default_status

def update_sync_status(blocks: int, is_full_sync: bool = False):
    """Update synchronization status file"""
    status_file = os.path.join(CONFIG['data_dir'], 'sync_status.json')
    
    try:
        current_status = get_sync_status()
        current_time = time.time()
        
        status_data = {
            'last_synced_blocks': blocks,
            'last_update_time': current_time,
            'last_full_sync_time': current_time if is_full_sync else current_status['last_full_sync_time'],
            'sync_count': current_status['sync_count'] + 1
        }
        
        os.makedirs(os.path.dirname(status_file), exist_ok=True)
        with open(status_file, 'w') as f:
            json.dump(status_data, f, indent=2)
            
    except Exception as e:
        safe_print(f"Error updating sync status: {e}")

def scan_existing_data() -> int:
    """Scan existing data files to determine last synced block"""
    try:
        if not os.path.exists(CONFIG['data_dir']):
            return 0
        
        max_page = 0
        for filename in os.listdir(CONFIG['data_dir']):
            if filename.startswith('page_') and filename.endswith('.json'):
                try:
                    page_num = int(filename[5:-5])  # Extract number from page_XXX.json
                    if page_file_exists(page_num):  # Verify file is valid
                        max_page = max(max_page, page_num)
                except ValueError:
                    continue
        
        return max_page * CONFIG['records_per_page']
        
    except Exception as e:
        safe_print(f"Error scanning existing data: {e}")
        return 0

def get_last_synced_blocks() -> int:
    """Get the last successfully synced block count"""
    status = get_sync_status()
    status_blocks = status['last_synced_blocks']
    
    if status_blocks > 0:
        return status_blocks
    
    # Fallback: scan existing files
    scanned_blocks = scan_existing_data()
    if scanned_blocks > 0:
        update_sync_status(scanned_blocks)
    
    return scanned_blocks

# ================================
# Synchronization Functions
# ================================
def perform_full_sync() -> bool:
    """Perform complete synchronization of all blocks"""
    safe_print("Starting full synchronization...")
    stats['start_time'] = time.time()
    
    try:
        # Reset statistics
        with stats_lock:
            stats['completed_pages'] = 0
            stats['skipped_pages'] = 0
            stats['failed_pages'] = set()
        
        # Get total blocks and calculate pages
        total_blocks = get_total_blocks()
        total_pages = total_blocks // CONFIG['records_per_page']
        stats['total_pages'] = total_pages
        
        safe_print(f"Total pages to sync: {total_pages:,}")
        
        if total_pages <= 0:
            safe_print("No pages to sync")
            return True
        
        # Generate page list and process in batches
        all_pages = list(range(1, total_pages + 1))
        failed_pages = batch_fetch_pages(all_pages)
        
        # Retry failed pages
        retry_round = 1
        while failed_pages and retry_round <= CONFIG['max_retries'] and not shutdown_event.is_set():
            safe_print(f"Retry round {retry_round}, retrying {len(failed_pages)} failed pages")
            failed_pages = batch_fetch_pages(failed_pages)
            retry_round += 1
        
        # Calculate final statistics
        elapsed_time = time.time() - stats['start_time']
        success_count = stats['completed_pages'] + stats['skipped_pages']
        
        safe_print(f"Full sync completed: {success_count}/{total_pages} pages, {len(failed_pages)} failures, {elapsed_time:.1f}s")
        
        # Update status
        stats['last_sync_time'] = time.time()
        stats['sync_count'] += 1
        stats['last_full_sync_time'] = time.time()
        
        update_sync_status(total_blocks, is_full_sync=True)
        
        return len(failed_pages) == 0
        
    except InterruptedError:
        safe_print("Full synchronization interrupted by shutdown request")
        return False
    except Exception as e:
        safe_print(f"Full synchronization failed: {e}")
        logger.exception("Full sync error details:")
        return False

def perform_incremental_sync(last_blocks: int, current_blocks: int) -> bool:
    """Perform incremental synchronization for new blocks"""
    if current_blocks <= last_blocks:
        return True
    
    try:
        # Calculate page range to sync
        last_page = last_blocks // CONFIG['records_per_page']
        current_page = current_blocks // CONFIG['records_per_page']
        
        # Determine pages to sync
        if current_page <= last_page:
            # New records in same page, re-fetch current page
            pages_to_sync = [current_page] if current_page > 0 else []
        else:
            # New pages available, sync from next page after last
            pages_to_sync = list(range(last_page + 1, current_page + 1))
        
        if not pages_to_sync:
            # Update status even if no new pages
            update_sync_status(current_blocks)
            return True
        
        new_blocks = current_blocks - last_blocks
        safe_print(f"Incremental sync: {new_blocks} new blocks, syncing {len(pages_to_sync)} pages")
        
        # For incremental sync, process pages directly (usually few pages)
        failed_pages = []
        for page in pages_to_sync:
            if shutdown_event.is_set():
                return False
            
            if not fetch_single_page(page):
                failed_pages.append(page)
        
        if failed_pages:
            safe_print(f"Incremental sync failed for pages: {failed_pages}")
            return False
        
        safe_print(f"Incremental sync completed successfully")
        update_sync_status(current_blocks)
        return True
        
    except Exception as e:
        safe_print(f"Incremental sync error: {e}")
        logger.exception("Incremental sync error details:")
        return False

def check_need_full_sync() -> bool:
    """Determine if full synchronization is required"""
    try:
        current_total = get_total_blocks()
        last_synced = get_last_synced_blocks()
        status = get_sync_status()
        
        # Check if never synced
        if last_synced == 0:
            safe_print("No previous sync detected, full sync required")
            return True
        
        # Check if gap is too large
        gap = current_total - last_synced
        if gap > CONFIG['full_sync_threshold']:
            safe_print(f"Block gap too large ({gap} blocks), full sync required")
            return True
        
        # Check if daily full sync is due
        last_full_sync = status.get('last_full_sync_time', 0)
        if time.time() - last_full_sync > CONFIG['daily_full_sync_interval']:
            safe_print("Daily full sync due")
            return True
            
        return False
        
    except Exception as e:
        safe_print(f"Error checking sync requirements: {e}")
        return True  # Default to full sync on error

# ================================
# Service Main Loop
# ================================
def incremental_sync_loop():
    """Main loop for incremental synchronization"""
    last_known_blocks = get_last_synced_blocks()
    last_full_sync_check = time.time()
    
    safe_print(f"Starting incremental sync mode from block {last_known_blocks}")
    
    while not shutdown_event.is_set():
        try:
            current_time = time.time()
            
            # Periodic check for full sync requirement
            if current_time - last_full_sync_check > CONFIG['daily_full_sync_interval']:
                if check_need_full_sync():
                    safe_print("Triggering periodic full sync")
                    success = perform_full_sync()
                    if success:
                        last_known_blocks = get_last_synced_blocks()
                last_full_sync_check = current_time
            
            # Check for new blocks
            current_total_blocks = get_total_blocks()
            
            if current_total_blocks > last_known_blocks:
                # New blocks detected
                new_blocks_count = current_total_blocks - last_known_blocks
                safe_print(f"Detected {new_blocks_count} new blocks")
                
                success = perform_incremental_sync(last_known_blocks, current_total_blocks)
                if success:
                    last_known_blocks = current_total_blocks
            elif current_total_blocks < last_known_blocks:
                # Blockchain reorganization detected
                safe_print("Potential blockchain reorganization detected, triggering full sync")
                if perform_full_sync():
                    last_known_blocks = get_last_synced_blocks()
            
            # Wait for next check interval
            if shutdown_event.wait(CONFIG['incremental_check_interval']):
                break
                
        except InterruptedError:
            break
        except Exception as e:
            safe_print(f"Incremental sync loop error: {e}")
            logger.exception("Incremental loop error details:")
            
            # Wait longer after errors
            if shutdown_event.wait(CONFIG['error_retry_interval']):
                break

def service_main_loop():
    """Main service loop with initial sync detection"""
    safe_print("XenBlocks fetcher service started")
    safe_print(f"Data directory: {CONFIG['data_dir']}")
    safe_print(f"Incremental check interval: {CONFIG['incremental_check_interval']}s")
    
    try:
        # Ensure data directory exists
        os.makedirs(CONFIG['data_dir'], exist_ok=True)
        
        # Check if initial full sync is needed
        if check_need_full_sync():
            safe_print("Performing initial full synchronization...")
            success = perform_full_sync()
            if not success and not shutdown_event.is_set():
                safe_print("Initial full sync failed, will retry in incremental mode")
        
        # Switch to incremental sync mode
        if not shutdown_event.is_set():
            safe_print("Switching to incremental synchronization mode...")
            incremental_sync_loop()
        
    except Exception as e:
        safe_print(f"Service main loop error: {e}")
        logger.exception("Service main loop error details:")
        return 1
    
    safe_print("XenBlocks fetcher service stopped")
    return 0

# ================================
# Main Entry Point
# ================================
def main():
    """Main function for service entry point"""
    try:
        # Log startup information
        safe_print("=" * 60)
        safe_print("XenBlocks Data Fetcher Service v2.0")
        safe_print(f"Max workers: {CONFIG['max_workers']}")
        safe_print(f"Batch size: {CONFIG['max_workers'] * CONFIG['batch_multiplier']}")
        safe_print(f"Data directory: {CONFIG['data_dir']}")
        safe_print("=" * 60)
        
        # Start service
        return service_main_loop()
        
    except KeyboardInterrupt:
        safe_print("Service interrupted by user")
        return 0
    except Exception as e:
        safe_print(f"Service startup failed: {e}")
        logger.exception("Service startup error details:")
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
