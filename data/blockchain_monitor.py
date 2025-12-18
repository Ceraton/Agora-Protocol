"""
Blockchain Monitor - On-Chain Analysis
Tracks whale wallets and large transfers to detect market movements early
"""
from typing import Dict, List, Optional
from utils.logger import setup_logger
from web3 import Web3
import os

class BlockchainMonitor:
    """
    Monitors blockchain for whale activity and large transfers.
    Now integrated with real Web3.py and ERC-20 support.
    """
    
    def __init__(self, network: str = 'ethereum'):
        self.logger = setup_logger("BlockchainMonitor")
        self.network = network
        
        # Watchlist
        self.watchlist = set()
        self.active_tokens = {} # symbol -> address map
        
        # Public RPCs (Fallbacks)
        self.rpc_urls = {
            'ethereum': [
                "https://eth.llamarpc.com",
                "https://cloudflare-eth.com",
                "https://rpc.ankr.com/eth"
            ]
        }
        
        # Known Token Addresses (Ethereum Mainnet)
        # In production, fetch from Token List API (Coingecko/Uniswap)
        raw_token_map = {
            'USDT': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
            'USDC': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
            'SHIB': '0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE',
            'LINK': '0x514910771AF9Ca656af840dff83E8264EcF986CA',
            'UNI': '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984',
            'MATI': '0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0', # Polygon (ERC20)
            'WBTC': '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
            'PEPE': '0x6982508145454Ce325dDBe47a25d4ec3d2311933',
            'LUNC': '0xbd31EA8212119f94A611FA969881CBa3EA06Fa8d' # Wrapped LUNC on ETH
        }
        
        # Ensure Checksums
        self.token_map = {}
        for sym, addr in raw_token_map.items():
            try:
                self.token_map[sym] = Web3.to_checksum_address(addr)
            except:
                self.logger.warning(f"Invalid address for {sym}: {addr}")
        
        # Minimal ERC-20 ABI for Transfer event
        self.erc20_abi = [
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "name": "from", "type": "address"},
                    {"indexed": True, "name": "to", "type": "address"},
                    {"indexed": False, "name": "value", "type": "uint256"}
                ],
                "name": "Transfer",
                "type": "event"
            }
        ]

        # Connect to Web3
        self.w3 = None
        self._connect_web3()
        
        # Whale wallets (Binance Hot Wallets, etc.)
        self.whale_wallets = {
            'ethereum': [
                '0x28C6c06298d514Db089934071355E5743bf21d60',  # Binance 14
                '0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549',  # Binance 15
                '0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb'   # Other Whale
            ]
        }
        
        self.transfer_threshold_usd = 1000000  # $1M+ USD equivalent
    
    def _connect_web3(self):
        """Attempts to connect to available RPC endpoints."""
        for url in self.rpc_urls.get(self.network, []):
            try:
                self.logger.info(f"Connecting to {url}...")
                w3 = Web3(Web3.HTTPProvider(url))
                if w3.is_connected():
                    self.w3 = w3
                    chain_id = w3.eth.chain_id
                    self.logger.info(f"Connected to {self.network} (Chain ID: {chain_id})")
                    return
            except Exception as e:
                self.logger.error(f"Failed to connect to {url}: {e}")
        
        self.logger.error("Could not connect to any Web3 RPC endpoint. On-Chain agent will be disabled.")

    def update_watchlist(self, symbols: List[str]):
        """
        Updates watchlist and resolves token addresses.
        """
        self.watchlist = set()
        self.active_tokens = {}
        
        # Always watch ETH
        self.watchlist.add('ETH')
        
        # Add Top 10 defaults
        defaults = ['BTC', 'SOL', 'XRP', 'DOGE', 'ADA']
        for d in defaults:
            self.watchlist.add(d)

        for s in symbols:
            base = s.split('/')[0].upper()
            self.watchlist.add(base)
            
            # Resolve to Token Address if exists
            # Handle substitutions (e.g. BTC -> WBTC) for ETH chain monitoring
            target_token = base
            if base == 'BTC': target_token = 'WBTC'
            
            if target_token in self.token_map:
                addr = self.token_map[target_token]
                self.active_tokens[base] = addr
                self.logger.info(f"Monitoring {base} via token {addr}")

    async def check_whale_activity(self, _unused_symbol: str = "") -> Dict:
        """
        Checks for whale wallet activity on the latest block for ALL watched assets.
        """
        if not self.w3:
            return {'whale_signal': 'neutral', 'confidence': 0.0, 'details': 'Web3 not connected'}

        try:
            # Mock pricing for threshold calculation
            # In prod, fetch real prices
            prices = {
                'ETH': 2200.0, 
                'BTC': 42000.0,
                'WBTC': 42000.0,
                'LUNC': 0.0001,
                'SHIB': 0.00001
            }
            
            # Get latest block
            block = self.w3.eth.get_block('latest', full_transactions=True)
            
            large_transfers = []
            net_flow_usd = 0.0
            
            # 1. Analyze Native ETH Transfers
            if 'ETH' in self.watchlist:
                threshold_eth = self.transfer_threshold_usd / prices['ETH']
                
                for tx in block.transactions[:50]: # Limit scan size
                    value_eth = float(self.w3.from_wei(tx['value'], 'ether'))
                    if value_eth > threshold_eth:
                        is_whale = (tx['from'] in self.whale_wallets.get(self.network, []) or 
                                   tx['to'] in self.whale_wallets.get(self.network, []))
                        
                        if is_whale or value_eth > (threshold_eth * 5):
                            large_transfers.append(f"ETH: {value_eth:.2f} ({'WHALE' if is_whale else 'Large'})")
                            if tx['to'] in self.whale_wallets.get(self.network, []):
                                net_flow_usd += value_eth * prices['ETH']
                            elif tx['from'] in self.whale_wallets.get(self.network, []):
                                net_flow_usd -= value_eth * prices['ETH']

            # 2. Analyze ERC-20 Token Transfers
            if self.active_tokens:
                # To scan logs effectively, we need the contract addresses
                # This is "expensive" (many RPC calls), so we limit to watching specific contracts
                # or scanning block logs if provider supports it.
                # LlamaRPC imposes limits. simple approach: check logs in block for our addresses.
                
                for receipt in block.transactions[:20]: # Check receipts of first 20 txs (saving RPC calls)
                    # Note: Checking receipts is actually 1 RPC call per TX -> 20 calls.
                    # Better: eth_getLogs for the block
                    pass

                # Optimized: Get logs for the entire block for our contracts
                # Limit to 5 tokens to prevent timeout
                watched_addrs = list(self.active_tokens.values())[:5]
                
                logs = self.w3.eth.get_logs({
                    'fromBlock': block.number,
                    'toBlock': block.number,
                    'address': watched_addrs
                })
                
                for log in logs:
                    # Check if Transfer event (topic[0] hash for Transfer)
                    transfer_topic = self.w3.keccak(text="Transfer(address,address,uint256)").hex()
                    
                    if log['topics'][0].hex() == transfer_topic:
                        # Decode
                        token_addr = log['address']
                        # Find symbol
                        symbol = next((k for k, v in self.active_tokens.items() if v.lower() == token_addr.lower()), "UNKNOWN")
                        
                        # Value is in data (uint256)
                        # Handle HexBytes or string
                        try:
                            if hasattr(log['data'], 'hex'):
                                value_raw = int(log['data'].hex(), 16)
                            elif isinstance(log['data'], bytes):
                                value_raw = int.from_bytes(log['data'], byteorder='big')
                            else:
                                value_raw = int(log['data'], 16)
                        except Exception as e:
                            self.logger.warning(f"Failed to decode log data: {e}")
                            continue

                        # Assume 18 decimals for simplicity (or 8 for WBTC, 6 for USDC)
                        decimals = 8 if symbol == 'WBTC' else (6 if symbol in ['USDC', 'USDT'] else 18)
                        value = value_raw / (10 ** decimals)
                        
                        price = prices.get(symbol, 1.0) # Default $1
                        value_usd = value * price
                        
                        if value_usd > self.transfer_threshold_usd:
                            large_transfers.append(f"{symbol}: {value:,.0f} (${value_usd/1e6:.1f}M)")
                            net_flow_usd += value_usd # Assume generic accumulation for now as we don't match whale addresses here yet

            # Determine Signal
            signal = 'neutral'
            confidence = 0.5
            details = "No significant on-chain activity"
            
            if len(large_transfers) > 0:
                details = f"Detected: {', '.join(large_transfers[:3])}"
                
                # Log to CSV
                self._log_alerts(large_transfers)
                
                if net_flow_usd > 0:
                    signal = 'bullish'
                    confidence = 0.6 + min(0.3, net_flow_usd / 10000000)
                elif net_flow_usd < 0:
                    signal = 'bearish'
                    confidence = 0.6 + min(0.3, abs(net_flow_usd) / 10000000)
                else:
                    details += " (High Volatility)"
            
            return {
                'whale_signal': signal,
                'confidence': confidence,
                'large_transfers': len(large_transfers),
                'net_flow': net_flow_usd,
                'details': details
            }
            
        except Exception as e:
            self.logger.error(f"Error reading blockchain: {e}")
            return {'whale_signal': 'neutral', 'confidence': 0.0, 'details': f'Error: {str(e)}'}

    def _log_alerts(self, alerts: List[str]):
        """Append alerts to CSV file"""
        try:
            from config import WHALE_ALERTS_FILE
            import time
            import csv
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(WHALE_ALERTS_FILE), exist_ok=True)
            
            # Timestamp
            ts = time.time()
            iso_ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
            
            with open(WHALE_ALERTS_FILE, 'a', newline='') as f:
                writer = csv.writer(f)
                # If empty, write header
                if f.tell() == 0:
                    writer.writerow(['Timestamp', 'Alert'])
                
                for alert in alerts:
                    writer.writerow([iso_ts, alert])
                    
        except Exception as e:
            self.logger.error(f"Failed to log whale alert: {e}")

    def add_whale_wallet(self, address: str):
        if self.network not in self.whale_wallets:
            self.whale_wallets[self.network] = []
        if address not in self.whale_wallets[self.network]:
            self.whale_wallets[self.network].append(address)

    def get_monitored_wallets(self) -> List[str]:
        return self.whale_wallets.get(self.network, [])
