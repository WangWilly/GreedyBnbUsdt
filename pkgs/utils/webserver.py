from aiohttp import web
import os
import aiofiles
import logging
from datetime import datetime
import psutil
import time

from pkgs.actioners.s1.actioner import ActionerS1
from pkgs.clients.exchange import ExchangeClient
from pkgs.managers.order.manager import ManagerOrder
from pkgs.managers.position.manager import ManagerPosition
from pkgs.traders.grid.trader import TraderGrid

################################################################################

class IPLogger:
    def __init__(self):
        self.ip_records = []  # Store IP access records
        self.max_records = 100  # Store a maximum of 100 records
        self._log_cache = {'content': None, 'timestamp': 0}  # Add log cache
        self._cache_ttl = 2  # Cache TTL (seconds)

    def add_record(self, ip, path):
        # Check if a record with the same IP exists
        for record in self.ip_records:
            if record['ip'] == ip:
                # If found, only update the time
                record['time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                record['path'] = path  # Update access path
                return
        
        # If it's a new IP, add a new record
        record = {
            'ip': ip,
            'path': path,
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        self.ip_records.append(record)
        
        # If the maximum number of records is exceeded, delete the oldest record
        if len(self.ip_records) > self.max_records:
            self.ip_records.pop(0)

    def get_records(self):
        return self.ip_records

################################################################################

def get_system_stats():
    """Get system resource usage"""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    memory_used = memory.used / (1024 * 1024 * 1024)  # Convert to GB
    memory_total = memory.total / (1024 * 1024 * 1024)
    return {
        'cpu_percent': cpu_percent,
        'memory_used': round(memory_used, 2),
        'memory_total': round(memory_total, 2),
        'memory_percent': memory.percent
    }

async def _read_log_content():
    """Common log reading function"""
    log_path = os.path.join(os.path.dirname(__file__), 'trading_system.log')
    if not os.path.exists(log_path):
        return None
        
    async with aiofiles.open(log_path, mode='r', encoding='utf-8') as f:
        content = await f.read()
        
    # Split logs by line and reverse order
    lines = content.strip().split('\n')
    lines.reverse()
    return '\n'.join(lines)

async def handle_log(request):
    try:
        # Record IP access
        ip = request.remote
        request.app['ip_logger'].add_record(ip, request.path)
        
        # Get system resource status
        system_stats = get_system_stats()
        
        # Read log content
        content = await _read_log_content()
        if content is None:
            return web.Response(text="Log file does not exist", status=404)
            
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Grid Trading Monitoring System</title>
            <meta charset="utf-8">
            <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
            <style>
                .grid-container {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 1rem;
                    padding: 1rem;
                }}
                .card {{
                    background: white;
                    border-radius: 0.5rem;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    padding: 1rem;
                }}
                .status-value {{
                    font-size: 1.5rem;
                    font-weight: bold;
                    color: #2563eb;
                }}
                .profit {{ color: #10b981; }}
                .loss {{ color: #ef4444; }}
                .log-container {{
                    height: calc(100vh - 400px);
                    overflow-y: auto;
                    background: #1e1e1e;
                    color: #d4d4d4;
                    padding: 1rem;
                    border-radius: 0.5rem;
                }}
            </style>
        </head>
        <body class="bg-gray-100">
            <div class="container mx-auto px-4 py-8">
                <h1 class="text-3xl font-bold mb-8 text-center text-gray-800">Grid Trading Monitoring System</h1>
                
                <!-- Status Cards -->
                <div class="grid-container mb-8">
                    <div class="card">
                        <h2 class="text-lg font-semibold mb-4">Basic Info & S1</h2>
                        <div class="space-y-2">
                            <div class="flex justify-between">
                                <span>Trading Pair</span>
                                <span class="status-value">{request.app['trader'].symbol}</span>
                            </div>
                            <div class="flex justify-between">
                                <span>Base Price</span>
                                <span class="status-value" id="base-price">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>Current Price (USDT)</span>
                                <span class="status-value" id="current-price">--</span>
                            </div>
                            <div class="flex justify-between pt-2 border-t mt-2">
                                <span>52-Day High (S1)</span>
                                <span class="status-value" id="s1-high">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>52-Day Low (S1)</span>
                                <span class="status-value" id="s1-low">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>Current Position (%)</span>
                                <span class="status-value" id="position-percentage">--</span>
                            </div>
                        </div>
                    </div>
                    
                    <div class="card">
                        <h2 class="text-lg font-semibold mb-4">Grid Parameters</h2>
                        <div class="space-y-2">
                            <div class="flex justify-between">
                                <span>Grid Size</span>
                                <span class="status-value" id="grid-size">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>Current Upper Band (USDT)</span>
                                <span class="status-value" id="grid-upper-band">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>Current Lower Band (USDT)</span>
                                <span class="status-value" id="grid-lower-band">--</span>
                            </div>    
                            <div class="flex justify-between">
                                <span>Trigger Threshold</span>
                                <span class="status-value" id="threshold">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>Target Order Amount</span>
                                <span class="status-value" id="target-order-amount">--</span>
                            </div>
                        </div>
                    </div>
                    
                    <div class="card">
                        <h2 class="text-lg font-semibold mb-4">Financial Status</h2>
                        <div class="space-y-2">
                            <div class="flex justify-between">
                                <span>Total Assets (USDT)</span>
                                <span class="status-value" id="total-assets">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>USDT Balance</span>
                                <span class="status-value" id="usdt-balance">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>BNB Balance</span>
                                <span class="status-value" id="bnb-balance">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>Total P&L (USDT)</span>
                                <span class="status-value" id="total-profit">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span>P&L Rate (%)</span>
                                <span class="status-value" id="profit-rate">--</span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- System Resource Monitoring -->
                <div class="card mb-8">
                    <h2 class="text-lg font-semibold mb-4">System Resources</h2>
                    <div class="grid grid-cols-2 gap-4">
                        <div class="p-4 bg-gray-50 rounded-lg">
                            <div class="text-sm text-gray-600">CPU Usage</div>
                            <div class="text-2xl font-bold mt-1">{system_stats['cpu_percent']}%</div>
                        </div>
                        <div class="p-4 bg-gray-50 rounded-lg">
                            <div class="text-sm text-gray-600">Memory Usage</div>
                            <div class="text-2xl font-bold mt-1">{system_stats['memory_percent']}%</div>
                            <div class="text-sm text-gray-500">
                                {system_stats['memory_used']}GB / {system_stats['memory_total']}GB
                            </div>
                        </div>
                        <div class="p-4 bg-gray-50 rounded-lg col-span-2">
                            <div class="text-sm text-gray-600">System Uptime</div>
                            <div class="text-xl font-bold mt-1" id="system-uptime">--</div>
                        </div>
                    </div>
                </div>

                <!-- Recent Trade History -->
                <div class="card mt-4 mb-8">
                    <h2 class="text-lg font-semibold mb-4">Recent Trades</h2>
                    <div class="overflow-x-auto">
                        <table class="min-w-full">
                            <thead>
                                <tr class="border-b">
                                    <th class="text-left py-2">Time</th>
                                    <th class="text-left py-2">Side</th>
                                    <th class="text-left py-2">Price</th>
                                    <th class="text-left py-2">Quantity</th>
                                    <th class="text-left py-2">Amount (USDT)</th>
                                </tr>
                            </thead>
                            <tbody id="trade-history">
                                <!-- Trade records will be dynamically inserted via JavaScript -->
                            </tbody>
                        </table>
                    </div>
                </div>

                <!-- IP Access Records -->
                <div class="card mb-8">
                    <h2 class="text-lg font-semibold mb-4">Access Records</h2>
                    <div class="overflow-x-auto">
                        <table class="min-w-full">
                            <thead>
                                <tr class="bg-gray-50">
                                    <th class="px-6 py-3 text-left">Time</th>
                                    <th class="px-6 py-3 text-left">IP Address</th>
                                    <th class="px-6 py-3 text-left">Access Path</th>
                                </tr>
                            </thead>
                            <tbody>
                                {''.join([f'''
                                <tr class="border-b">
                                    <td class="px-6 py-4">{record["time"]}</td>
                                    <td class="px-6 py-4">{record["ip"]}</td>
                                    <td class="px-6 py-4">{record["path"]}</td>
                                </tr>
                                ''' for record in list(reversed(request.app['ip_logger'].get_records()))[:5]])}
                            </tbody>
                        </table>
                    </div>
                </div>

                <!-- System Logs -->
                <div class="card">
                    <h2 class="text-lg font-semibold mb-4">System Logs</h2>
                    <div class="log-container" id="log-content">
                        <pre>{content}</pre>
                    </div>
                </div>
            </div>

            <script>
                async function updateStatus() {{
                    try {{
                        const response = await fetch('/api/status');
                        const data = await response.json();
                        
                        if (data.error) {{
                            console.error('Failed to get status:', data.error);
                            return;
                        }}
                        
                        // Update basic info
                        document.querySelector('#base-price').textContent = 
                            data.base_price ? data.base_price.toFixed(2) + ' USDT' : '--';
                        
                        // Update current price
                        document.querySelector('#current-price').textContent = 
                            data.current_price ? data.current_price.toFixed(2) : '--';
                        
                        // Update S1 info and position
                        document.querySelector('#s1-high').textContent = 
                            data.s1_daily_high ? data.s1_daily_high.toFixed(2) : '--';
                        document.querySelector('#s1-low').textContent = 
                            data.s1_daily_low ? data.s1_daily_low.toFixed(2) : '--';
                        document.querySelector('#position-percentage').textContent = 
                            data.position_percentage != null ? data.position_percentage.toFixed(2) + '%' : '--';
                        
                        // Update grid parameters
                        document.querySelector('#grid-size').textContent = 
                            data.grid_size ? (data.grid_size * 100).toFixed(2) + '%' : '--';
                        document.querySelector('#threshold').textContent = 
                            data.threshold ? (data.threshold * 100).toFixed(2) + '%' : '--';

                        // ---> NEW: Update grid upper/lower bands <---
                        document.querySelector('#grid-upper-band').textContent =
                            data.grid_upper_band != null ? data.grid_upper_band.toFixed(2) : '--';
                        document.querySelector('#grid-lower-band').textContent =
                            data.grid_lower_band != null ? data.grid_lower_band.toFixed(2) : '--';
                        
                        // Update financial status
                        document.querySelector('#total-assets').textContent = 
                            data.total_assets ? data.total_assets.toFixed(2) + ' USDT' : '--';
                        document.querySelector('#usdt-balance').textContent = 
                            data.usdt_balance != null ? data.usdt_balance.toFixed(2) : '--';
                        document.querySelector('#bnb-balance').textContent = 
                            data.bnb_balance != null ? data.bnb_balance.toFixed(4) : '--';
                        
                        // Update P&L info
                        const totalProfitElement = document.querySelector('#total-profit');
                        totalProfitElement.textContent = data.total_profit ? data.total_profit.toFixed(2) : '--';
                        totalProfitElement.className = `status-value ${{data.total_profit >= 0 ? 'profit' : 'loss'}}`;

                        const profitRateElement = document.querySelector('#profit-rate');
                        profitRateElement.textContent = data.profit_rate ? data.profit_rate.toFixed(2) + '%' : '--';
                        profitRateElement.className = `status-value ${{data.profit_rate >= 0 ? 'profit' : 'loss'}}`;
                        
                        // Update trade history
                        document.querySelector('#trade-history').innerHTML = data.trade_history.map(function(trade) {{ return ` 
                            <tr class="border-b">
                                <td class="py-2">${{trade.timestamp}}</td>
                                <td class="py-2 ${{trade.side === 'buy' ? 'text-green-500' : 'text-red-500'}}">
                                    ${{trade.side === 'buy' ? 'Buy' : 'Sell'}}
                                </td>
                                <td class="py-2">${{parseFloat(trade.price).toFixed(2)}}</td>
                                <td class="py-2">${{parseFloat(trade.amount).toFixed(4)}}</td>
                                <td class="py-2">${{(parseFloat(trade.price) * parseFloat(trade.amount)).toFixed(2)}}</td>
                            </tr>
                        `; }}).join('');
                        
                        // Update target order amount
                        document.querySelector('#target-order-amount').textContent = 
                            data.target_order_amount ? data.target_order_amount.toFixed(2) + ' USDT' : '--';
                        
                        // Update system uptime
                        document.querySelector('#system-uptime').textContent = data.uptime;
                        
                        console.log('Status updated successfully:', data);
                    }} catch (error) {{
                        console.error('Failed to update status:', error);
                    }}
                }}

                // Update status every 2 seconds
                setInterval(updateStatus, 2000);
                
                // Update status immediately on page load
                updateStatus();
            </script>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')
    except Exception as e:
        return web.Response(text=f"Error: {str(e)}", status=500)

async def handle_status(request):
    """Handle status API requests"""
    try:
        trader: TraderGrid = request.app['trader']
        exchange: ExchangeClient = request.app['exchange']
        position_manager: ManagerPosition = request.app['position_manager']
        s1_controller: ActionerS1 = request.app['actioner_s1']

        # Get exchange data
        balance = await exchange.fetch_balance()
        current_price = await position_manager.get_latest_price() or 0 # Provide default value in case of failure
        
        # Get funding account balance
        funding_balance = await exchange.fetch_funding_balance()
        
        # Get grid parameters
        grid_size = trader.grid_size
        grid_size_decimal = grid_size / 100 if grid_size else 0
        threshold = grid_size_decimal / 5
        
        # ---> NEW: Calculate grid upper/lower bands <---
        # Ensure trader.base_price and trader.grid_size are valid
        upper_band = None
        lower_band = None
        if trader.base_price is not None and trader.grid_size is not None:
             try:
                 # Call existing methods in trader.py
                 upper_band = trader._get_upper_band()
                 lower_band = trader._get_lower_band()
             except Exception as band_e:
                 logging.warning(f"Failed to calculate grid upper/lower bands: {band_e}")
        
        # Calculate system uptime
        current_time = time.time()
        uptime_seconds = int(current_time - trader.start_time)
        days, remainder = divmod(uptime_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
        
        # Calculate total assets
        bnb_balance = float(balance['total'].get('BNB', 0))
        usdt_balance = float(balance['total'].get('USDT', 0))
        total_assets = usdt_balance + (bnb_balance * current_price)
        
        # Calculate total P&L and P&L rate
        initial_principal = 0
        total_profit = 0.0
        profit_rate = 0.0
        if initial_principal > 0:
            total_profit = total_assets - initial_principal
            profit_rate = (total_profit / initial_principal) * 100
        else:
            logging.warning("Initial principal not set or is 0, cannot calculate P&L rate")
        
        # Get recent trade info
        last_trade_price = trader.last_trade_price
        last_trade_time = trader.last_trade_time
        last_trade_time_str = datetime.fromtimestamp(last_trade_time).strftime('%Y-%m-%d %H:%M:%S') if last_trade_time else '--'
        
        # Get trade history
        trade_history = []
        order_manager: ManagerOrder = request.app['order_manager']
        trades = order_manager.get_trade_history()
        trade_history = [{
            'timestamp': datetime.fromtimestamp(trade['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
            'side': trade.get('side', '--'),
            'price': trade.get('price', 0),
            'amount': trade.get('amount', 0),
            'profit': trade.get('profit', 0)
        } for trade in trades[-10:]]  # Take only the last 10 trades
        
        # Calculate target order amount (10% of total assets)
        target_order_amount = await trader._calculate_order_amount('buy') # buy/sell result is the same
        
        # Get position percentage - use risk manager's method to get the most accurate position ratio
        position_ratio = await position_manager.get_position_ratio()
        position_percentage = position_ratio * 100
        
        # Get S1 high/low prices
        s1_high = s1_controller.s1_daily_high if s1_controller else None
        s1_low = s1_controller.s1_daily_low if s1_controller else None
        
        # Build response data
        status = {
            "base_price": trader.base_price,
            "current_price": current_price,
            "grid_size": grid_size_decimal,
            "threshold": threshold,
            "total_assets": total_assets,
            "usdt_balance": usdt_balance,
            "bnb_balance": bnb_balance,
            "target_order_amount": target_order_amount,
            "trade_history": trade_history or [],
            "last_trade_price": last_trade_price,
            "last_trade_time": last_trade_time,
            "last_trade_time_str": last_trade_time_str,
            "total_profit": total_profit,
            "profit_rate": profit_rate,
            "s1_daily_high": s1_high,
            "s1_daily_low": s1_low,
            "position_percentage": position_percentage,
            # ---> NEW: Add upper/lower bands to response data <---
            "grid_upper_band": upper_band,
            "grid_lower_band": lower_band,
            "uptime": uptime_str, # Add uptime string
            "uptime_seconds": uptime_seconds # Add uptime seconds for calculation
        }
        
        return web.json_response(status)
    except Exception as e:
        logging.error(f"Failed to get status data: {str(e)}", exc_info=True)
        return web.json_response({"error": str(e)}, status=500)

async def start_web_server(trader: TraderGrid, exchange: ExchangeClient, position_manager: ManagerPosition, actioner_s1: ActionerS1, order_manager: ManagerOrder):
    app = web.Application()
    # Add middleware to handle invalid requests
    @web.middleware
    async def error_middleware(request, handler):
        try:
            return await handler(request)
        except web.HTTPException as ex:
            return web.json_response(
                {"error": str(ex)},
                status=ex.status,
                headers={'Access-Control-Allow-Origin': '*'}
            )
        except Exception as e:
            return web.json_response(
                {"error": "Internal Server Error"},
                status=500,
                headers={'Access-Control-Allow-Origin': '*'}
            )
    
    app.middlewares.append(error_middleware)
    app['trader'] = trader
    app['exchange'] = exchange
    app['position_manager'] = position_manager
    app['actioner_s1'] = actioner_s1
    app['order_manager'] = order_manager
    app['ip_logger'] = IPLogger()
    
    # Disable access log
    logging.getLogger('aiohttp.access').setLevel(logging.WARNING)

    home_prefix = os.getenv('HOME_PREFIX', '')
    
    app.router.add_get('/' + home_prefix, handle_log)
    app.router.add_get('/api/logs', handle_log_content)
    app.router.add_get('/api/status', handle_status)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 58181)
    await site.start()

    # Print access addresses
    local_ip = "localhost"  # Or use actual IP
    logging.info(f"Web server started:")
    logging.info(f"- Local access: http://{local_ip}:58181/{home_prefix}")
    logging.info(f"- LAN access: http://0.0.0.0:58181/{home_prefix}")

async def handle_log_content(request):
    """API endpoint that returns only log content"""
    try:
        content = await _read_log_content()
        if content is None:
            return web.Response(text="", status=404)
            
        return web.Response(text=content)
    except Exception as e:
        return web.Response(text="", status=500)
