import asyncio
import time
import numpy as np

from pydantic import Field
from pydantic_settings import BaseSettings

from pkgs.actioners.s1.actioner import ActionerS1
from pkgs.managers.advancerisk.manager import ManagerAdvancedRisk
from pkgs.managers.position.manager import ManagerPosition
from pkgs.utils.logging import get_logger_named
from pkgs.clients.exchange import ExchangeClient
from pkgs.managers.order.manager import ManagerOrder

################################################################################

PREFIX = "TRADER_GRID_"


class TraderGridConfig(BaseSettings):
    SYMBOL: str = Field(
        default="BNB/USDT", alias=PREFIX + "SYMBOL", description="Trading symbol"
    )
    INITIAL_BASE_PRICE: float = Field(
        default=0.0,
        alias=PREFIX + "INITIAL_BASE_PRICE",
        description="Initial base price for grid trading",
    )
    INITIAL_GRID: float = Field(
        default=2.0,
        alias=PREFIX + "INITIAL_GRID",
        description="Initial grid size in percentage",
    )

    AUTO_ADJUST_BASE_PRICE: bool = Field(
        default=False,
        alias=PREFIX + "AUTO_ADJUST_BASE_PRICE",
        description="Automatically adjust base price based on market conditions",
    )
    MIN_POSITION_RATIO: float = Field(
        default=0.1,
        alias=PREFIX + "MIN_POSITION_RATIO",
        description="Minimum position ratio to trigger base price adjustment",
    )
    MAX_POSITION_RATIO: float = Field(
        default=0.9,
        alias=PREFIX + "MAX_POSITION_RATIO",
        description="Maximum position ratio to trigger base price adjustment",
    )

    BASE_AMOUNT: float = Field(
        default=50.0,
        alias=PREFIX + "BASE_AMOUNT",
        description="Base amount for each grid trade in BNB",
    )
    MIN_TRADE_AMOUNT: float = Field(
        default=20.0,
        alias=PREFIX + "MIN_TRADE_AMOUNT",
        description="Minimum trade amount in USDT",
    )

    VOLATILITY_WINDOW: int = Field(
        default=24,
        alias=PREFIX + "VOLATILITY_WINDOW",
        description="Volatility calculation window in hours",
    )

    RISK_FACTOR: float = Field(
        default=0.1,
        alias=PREFIX + "RISK_FACTOR",
        description="Risk factor for position sizing",
    )
    SAFETY_MARGIN: float = Field(
        default=0.95,
        alias=PREFIX + "SAFETY_MARGIN",
        description="Safety margin for balance calculations",
    )

    @property
    def DYNAMIC_INTERVAL_PARAMS(self):
        return {
            # 定义波动率区间与对应调整间隔（小时）的映射关系
            "volatility_to_interval_hours": [
                # 格式: {'range': [最低波动率(含), 最高波动率(不含)], 'interval_hours': 对应的小时间隔}
                {
                    "range": [0, 0.20],
                    "interval_hours": 1.0,
                },  # 波动率 < 0.20 时，间隔 1 小时
                {
                    "range": [0.20, 0.40],
                    "interval_hours": 0.5,
                },  # 波动率 0.20 到 0.40 时，间隔30分钟
                {
                    "range": [0.40, 0.80],
                    "interval_hours": 0.25,
                },  # 波动率 0.40 到 0.80 时，间隔15分钟
                {
                    "range": [0.80, 999],
                    "interval_hours": 0.125,
                },  # 波动率 >=0.80 ，间隔7.5分钟
            ],
            # 定义一个默认间隔，以防波动率计算失败或未匹配到任何区间
            "default_interval_hours": 1.0,
        }

    @property
    def GRID_PARAMS(self):
        return {
            "initial": self.INITIAL_GRID,
            "min": 1.0,
            "max": 4.0,
            "volatility_threshold": {
                "ranges": [
                    {"range": [0, 0.20], "grid": 1.0},  # 波动率 0-20%，网格1.0%
                    {"range": [0.20, 0.40], "grid": 1.5},  # 波动率 20-40%，网格1.5%
                    {"range": [0.40, 0.60], "grid": 2.0},  # 波动率 40-60%，网格2.0%
                    {"range": [0.60, 0.80], "grid": 2.5},  # 波动率 60-80%，网格2.5%
                    {"range": [0.80, 1.00], "grid": 3.0},  # 波动率 80-100%，网格3.0%
                    {"range": [1.00, 1.20], "grid": 3.5},  # 波动率 100-120%，网格3.5%
                    {"range": [1.20, 999], "grid": 4.0},  # 波动率 >120%，网格4.0%
                ]
            },
        }

    @staticmethod
    def flip_threshold(grid_size: float) -> float:
        return (grid_size / 5) / 100


################################################################################


class TraderGrid:
    def __init__(
        self,
        config: TraderGridConfig,
        exchange: ExchangeClient,
        position_manager: ManagerPosition,
        risk_manager: ManagerAdvancedRisk,
        actioner_s1: ActionerS1,
        order_manager: ManagerOrder,
    ):
        self.logger = get_logger_named("TraderGrid")
        self.exchange = exchange
        self.cfg: TraderGridConfig = config

        ############################################################################

        self.base_price = config.INITIAL_BASE_PRICE
        self.grid_size = config.INITIAL_GRID

        self.ORDER_TIMEOUT = 10  # 订单超时时间（秒）

        # trader states
        self.initialized = False

        # runtime states
        self.highest = None
        self.lowest = None
        self.current_price = None
        self.active_orders = {"buy": None, "sell": None}

        self.pending_orders = {}
        self.order_timestamps = {}

        self.buying_or_selling = False  # 不在等待买入或卖出

        # info
        # self.total_assets = 0 # Will be fetched from position_manager
        self.last_trade_time = None
        self.last_trade_price = None
        self.last_grid_adjust_time = time.time()

        # runtime helpers
        self.position_manager = position_manager
        self.risk_manager = risk_manager
        self.actioner_s1 = actioner_s1
        self.order_manager = order_manager

    ############################################################################
    # initialization and setup

    async def initialize(self):
        if self.initialized:
            return

        self.logger.info("Initializing TraderGrid...")
        try:
            # Initialize position manager first
            if not await self.position_manager.initialize():
                raise RuntimeError("Failed to initialize position manager")

            # 检查现货账户资金并划转
            await self._check_and_transfer_initial_funds()

            # Set base price if not provided
            if self.base_price <= 0:
                self.base_price = await self.position_manager.get_latest_price()
                self.logger.info(f"Apply latest price as base price: {self.base_price}")

            if self.base_price is None or self.base_price <= 0:
                raise ValueError(
                    "Failed to fetch initial base price, please check the market data."
                )

            self.logger.info(f"Initial base price set to: {self.base_price} USDT")

            # 获取并更新最新的10条交易记录
            try:
                self.logger.info("正在获取最近10条交易记录...")
                latest_trades = await self.exchange.fetch_my_trades(
                    self.cfg.SYMBOL, limit=10
                )
                if latest_trades:
                    # 转换格式以匹配 OrderTracker 期望的格式 (如果需要)
                    formatted_trades = []
                    for trade in latest_trades:
                        # 注意: ccxt 返回的 trade 结构可能需要调整
                        # 假设 OrderTracker 需要 timestamp(秒), side, price, amount, profit, order_id
                        # profit 可能需要后续计算或默认为0
                        formatted_trade = {
                            "timestamp": trade["timestamp"] / 1000,  # ms to s
                            "side": trade["side"],
                            "price": trade["price"],
                            "amount": trade["amount"],
                            "cost": trade["cost"],  # 保留原始 cost
                            "fee": trade.get("fee", {}).get("cost", 0),  # 提取手续费
                            "order_id": trade.get("order"),  # 关联订单ID
                            "profit": 0,  # 初始化时设为0，或者后续计算
                        }
                        formatted_trades.append(formatted_trade)

                    # 直接替换 OrderTracker 中的历史记录
                    self.order_manager.trade_history = formatted_trades
                    self.order_manager.save_trade_history()  # 保存到文件
                    self.logger.info(
                        f"已使用最新的 {len(formatted_trades)} 条交易记录更新历史。"
                    )
                else:
                    self.logger.info("未能获取到最新的交易记录，将使用本地历史。")
            except Exception as trade_fetch_error:
                self.logger.error(f"获取或处理最新交易记录时出错: {trade_fetch_error}")

            self.initialized = True
        except Exception as e:
            self.initialized = False
            self.logger.error(f"初始化失败: {str(e)}")
            raise

    async def _check_and_transfer_initial_funds(self):
        """检查并划转初始资金"""
        try:
            # 获取现货和理财账户余额
            balance = (
                await self.exchange.fetch_balance()
            )  # Keep for direct spot balance access
            funding_balance = (
                await self.exchange.fetch_funding_balance()
            )  # Keep for direct funding balance access
            total_assets = await self.position_manager.get_total_assets()
            current_price = await self.position_manager.get_latest_price()

            # 计算目标持仓（总资产的16%）
            target_usdt = total_assets * 0.16
            target_bnb = (total_assets * 0.16) / current_price

            # 获取现货余额
            usdt_balance = float(balance["free"].get("USDT", 0))
            bnb_balance = float(balance["free"].get("BNB", 0))

            # 计算总余额（现货+理财）
            total_usdt = usdt_balance + float(funding_balance.get("USDT", 0))
            total_bnb = bnb_balance + float(funding_balance.get("BNB", 0))

            # 调整USDT余额
            if usdt_balance > target_usdt:
                # 多余的申购到理财
                transfer_amount = usdt_balance - target_usdt
                self.logger.info(f"发现可划转USDT: {transfer_amount}")
                # --- 添加最小申购金额检查 (>= 1 USDT) ---
                if transfer_amount >= 1.0:
                    try:
                        await self.exchange.transfer_to_savings("USDT", transfer_amount)
                        self.logger.info(f"已将 {transfer_amount:.2f} USDT 申购到理财")
                    except Exception as e_savings_usdt:
                        self.logger.error(f"申购USDT到理财失败: {str(e_savings_usdt)}")
                else:
                    self.logger.info(
                        f"可划转USDT ({transfer_amount:.2f}) 低于最小申购额 1.0 USDT，跳过申购"
                    )
            elif usdt_balance < target_usdt:
                # 不足的从理财赎回
                transfer_amount = target_usdt - usdt_balance
                self.logger.info(f"从理财赎回USDT: {transfer_amount}")
                # 同样，赎回USDT也可能需要最小金额检查，如果遇到错误需添加
                try:
                    await self.exchange.transfer_to_spot("USDT", transfer_amount)
                    self.logger.info(f"已从理财赎回 {transfer_amount:.2f} USDT")
                except Exception as e_spot_usdt:
                    self.logger.error(f"从理财赎回USDT失败: {str(e_spot_usdt)}")

            # 调整BNB余额
            if bnb_balance > target_bnb:
                # 多余的申购到理财
                transfer_amount = bnb_balance - target_bnb
                self.logger.info(f"发现可划转BNB: {transfer_amount}")
                # --- 添加最小申购金额检查 ---
                if transfer_amount >= 0.01:
                    try:
                        await self.exchange.transfer_to_savings("BNB", transfer_amount)
                        self.logger.info(f"已将 {transfer_amount:.4f} BNB 申购到理财")
                    except Exception as e_savings:
                        self.logger.error(f"申购BNB到理财失败: {str(e_savings)}")
                else:
                    self.logger.info(
                        f"可划转BNB ({transfer_amount:.4f}) 低于最小申购额 0.01 BNB，跳过申购"
                    )
            elif bnb_balance < target_bnb:
                # 不足的从理财赎回
                transfer_amount = target_bnb - bnb_balance
                self.logger.info(f"从理财赎回BNB: {transfer_amount}")
                # 赎回操作通常有不同的最低限额，或者限额较低，这里暂时不加检查
                # 如果赎回也遇到 -6005，需要在这里也加上对应的赎回最小额检查
                try:
                    await self.exchange.transfer_to_spot("BNB", transfer_amount)
                    self.logger.info(f"已从理财赎回 {transfer_amount:.4f} BNB")
                except Exception as e_spot:
                    self.logger.error(f"从理财赎回BNB失败: {str(e_spot)}")

            self.logger.info(
                f"资金分配完成\n" f"USDT: {total_usdt:.2f}\n" f"BNB: {total_bnb:.4f}"
            )
        except Exception as e:
            self.logger.error(f"初始资金检查失败: {str(e)}")

    ############################################################################

    async def _calculate_dynamic_interval_seconds(self):
        """根据波动率动态计算网格调整的时间间隔（秒）"""
        try:
            volatility = await self._calculate_volatility()
            if volatility is None:  # Handle case where volatility calculation failed
                raise ValueError("波动率计算失败")  # Volatility calculation failed

            interval_rules = self.cfg.DYNAMIC_INTERVAL_PARAMS[
                "volatility_to_interval_hours"
            ]
            default_interval_hours = self.cfg.DYNAMIC_INTERVAL_PARAMS[
                "default_interval_hours"
            ]

            matched_interval_hours = default_interval_hours  # Start with default

            for rule in interval_rules:
                vol_range = rule["range"]
                # Check if volatility falls within the defined range [min, max)
                if vol_range[0] <= volatility < vol_range[1]:
                    matched_interval_hours = rule["interval_hours"]
                    self.logger.debug(
                        f"动态间隔匹配: 波动率 {volatility:.4f} 在范围 {vol_range}, 间隔 {matched_interval_hours} 小时"
                    )  # Dynamic interval match
                    break  # Stop after first match

            interval_seconds = matched_interval_hours * 3600
            # Add a minimum interval safety check
            min_interval_seconds = 5 * 60  # Example: minimum 5 minutes
            final_interval_seconds = max(interval_seconds, min_interval_seconds)

            self.logger.debug(
                f"计算出的动态调整间隔: {final_interval_seconds:.0f} 秒 ({final_interval_seconds/3600:.2f} 小时)"
            )  # Calculated dynamic adjustment interval
            return final_interval_seconds

        except Exception as e:
            self.logger.error(
                f"计算动态调整间隔失败: {e}, 使用默认间隔。"
            )  # Failed to calculate dynamic interval, using default.
            # Fallback to default interval from config
            default_interval_hours = self.cfg.DYNAMIC_INTERVAL_PARAMS.get(
                "default_interval_hours", 1.0
            )
            return default_interval_hours * 3600

    async def adjust_grid_size(self):
        """根据波动率和市场趋势调整网格大小"""
        try:
            volatility = await self._calculate_volatility()
            self.logger.info(f"当前波动率: {volatility:.4f}")

            # 根据波动率获取基础网格大小
            base_grid = None
            for range_config in self.cfg.GRID_PARAMS["volatility_threshold"]["ranges"]:
                if range_config["range"][0] <= volatility < range_config["range"][1]:
                    base_grid = range_config["grid"]
                    break

            # 如果没有匹配到波动率范围，使用默认网格
            if base_grid is None:
                base_grid = self.cfg.INITIAL_GRID

            # 删除趋势调整逻辑
            new_grid = base_grid

            # 确保网格在允许范围内
            new_grid = max(
                min(new_grid, self.cfg.GRID_PARAMS["max"]), self.cfg.GRID_PARAMS["min"]
            )

            if new_grid != self.grid_size:
                self.logger.info(
                    f"调整网格大小 | "
                    f"波动率: {volatility:.2%} | "
                    f"原网格: {self.grid_size:.2f}% | "
                    f"新网格: {new_grid:.2f}%"
                )
                self.grid_size = new_grid

        except Exception as e:
            self.logger.error(f"调整网格大小失败: {str(e)}")

    async def _calculate_volatility(self):
        """计算价格波动率"""
        try:
            # 获取24小时K线数据
            klines = await self.exchange.fetch_ohlcv(
                self.cfg.SYMBOL, timeframe="1h", limit=self.cfg.VOLATILITY_WINDOW
            )

            if not klines:
                return 0

            # 计算收益率
            prices = [float(k[4]) for k in klines]  # 收盘价
            returns = np.diff(np.log(prices))

            # 计算波动率（标准差）并年化
            volatility = np.std(returns) * np.sqrt(24 * 365)  # 年化波动率
            return volatility

        except Exception as e:
            self.logger.error(f"计算波动率失败: {str(e)}")
            return 0

    ############################################################################

    async def _calculate_order_amount(self):
        """计算目标订单金额 (总资产的10%)\n"""
        try:
            current_time = time.time()

            # 使用缓存避免频繁计算和日志输出
            cache_key = f"order_amount_target"  # 使用不同的缓存键
            if (
                hasattr(self, cache_key)
                and current_time - getattr(self, f"{cache_key}_time") < 60
            ):  # 1分钟缓存
                return getattr(self, cache_key)

            total_assets = await self.position_manager.get_total_assets()

            # 目标金额严格等于总资产的10%
            amount = total_assets * 0.1

            # 只在金额变化超过1%时记录日志
            # 使用 max(..., 0.01) 避免除以零错误
            if (
                not hasattr(self, f"{cache_key}_last")
                or abs(amount - getattr(self, f"{cache_key}_last", 0))
                / max(getattr(self, f"{cache_key}_last", 0.01), 0.01)
                > 0.01
            ):
                self.logger.info(
                    f"目标订单金额计算 | "
                    f"总资产: {total_assets:.2f} USDT | "
                    f"计算金额 (10%): {amount:.2f} USDT"
                )
                setattr(self, f"{cache_key}_last", amount)

            # 更新缓存
            setattr(self, cache_key, amount)
            setattr(self, f"{cache_key}_time", current_time)

            return amount

        except Exception as e:
            self.logger.error(f"计算目标订单金额失败: {str(e)}")
            # 返回一个合理的默认值或上次缓存值，避免返回0导致后续计算错误
            return getattr(self, cache_key, 0)  # 如果缓存存在则返回缓存，否则返回0

    ############################################################################

    async def execute_order(self, side):
        """执行订单，带重试机制"""
        max_retries = 10  # 最大重试次数
        retry_count = 0
        check_interval = 3  # 下单后等待检查时间（秒）

        while retry_count < max_retries:
            try:
                # 获取最新订单簿数据
                order_book = await self.exchange.fetch_order_book(
                    self.cfg.SYMBOL, limit=5
                )
                if (
                    not order_book
                    or not order_book.get("asks")
                    or not order_book.get("bids")
                ):
                    self.logger.error("获取订单簿数据失败或数据不完整")
                    retry_count += 1
                    await asyncio.sleep(3)
                    continue

                # 使用买1/卖1价格
                if side == "buy":
                    order_price = order_book["asks"][0][0]  # 卖1价买入
                else:
                    order_price = order_book["bids"][0][0]  # 买1价卖出

                # 计算交易数量
                amount_usdt = await self._calculate_order_amount()
                amount = self.position_manager.adjust_amount_precision(
                    amount_usdt / order_price
                )

                # 检查余额是否足够
                # ensure_trading_funds can be called here with specific amounts
                if not await self.position_manager.ensure_trading_funds(
                    side, amount_usdt
                ):
                    self.logger.warning(
                        f"{side.capitalize()}余额不足或划转失败，第 {retry_count + 1} 次尝试中止"
                    )
                    return False

                self.logger.info(
                    f"尝试第 {retry_count + 1}/{max_retries} 次 {side} 单 | "
                    f"价格: {order_price} | "
                    f"金额: {amount_usdt:.2f} USDT | "
                    f"数量: {amount:.8f} BNB"
                )

                # 创建订单
                order = await self.exchange.create_order(
                    self.cfg.SYMBOL, "limit", side, amount, order_price
                )

                # 更新活跃订单状态
                order_id = order["id"]
                self.active_orders[side] = order_id
                self.order_manager.add_order(order)

                # 等待指定时间后检查订单状态
                self.logger.info(f"订单已提交，等待 {check_interval} 秒后检查状态")
                await asyncio.sleep(check_interval)

                # 检查订单状态
                updated_order = await self.exchange.fetch_order(
                    order_id, self.cfg.SYMBOL
                )

                # 订单已成交
                if updated_order["status"] == "closed":
                    self.logger.info(f"订单已成交 | ID: {order_id}")
                    # 更新基准价
                    self.base_price = float(updated_order["price"])
                    # 清除活跃订单状态
                    self.active_orders[side] = None

                    # 更新交易记录
                    trade_info = {
                        "timestamp": time.time(),
                        "side": side,
                        "price": float(updated_order["price"]),
                        "amount": float(updated_order["filled"]),
                        "order_id": updated_order["id"],
                    }
                    self.order_manager.add_trade(trade_info)

                    # 更新最后交易时间和价格
                    self.last_trade_time = time.time()
                    self.last_trade_price = float(updated_order["price"])

                    self.logger.info(f"基准价已更新: {self.base_price}")

                    # 发送通知
                    # 使用更清晰的格式发送交易成功消息
                    trade_side = "buy" if side == "buy" else "sell"
                    trade_price = float(updated_order["price"])
                    trade_amount = float(updated_order["filled"])
                    trade_total = trade_price * trade_amount

                    # 交易完成后，检查并转移多余资金到理财
                    await self.position_manager.transfer_excess_funds()

                    return updated_order

                # 如果订单未成交，取消订单并重试
                self.logger.warning(
                    f"订单未成交，尝试取消 | ID: {order_id} | 状态: {updated_order['status']}"
                )
                try:
                    await self.exchange.cancel_order(order_id, self.cfg.SYMBOL)
                    self.logger.info(f"订单已取消，准备重试 | ID: {order_id}")
                except Exception as e:
                    # 如果取消订单时出错，检查是否已成交
                    self.logger.warning(f"取消订单时出错: {str(e)}，再次检查订单状态")
                    try:
                        check_order = await self.exchange.fetch_order(
                            order_id, self.cfg.SYMBOL
                        )
                        if check_order["status"] == "closed":
                            self.logger.info(f"订单已经成交 | ID: {order_id}")
                            # 处理已成交的订单（与上面相同的逻辑）
                            self.base_price = float(check_order["price"])
                            self.active_orders[side] = None
                            trade_info = {
                                "timestamp": time.time(),
                                "side": side,
                                "price": float(check_order["price"]),
                                "amount": float(check_order["filled"]),
                                "order_id": check_order["id"],
                            }
                            self.order_manager.add_trade(trade_info)
                            self.last_trade_time = time.time()
                            self.last_trade_price = float(check_order["price"])
                            await self.position_manager.transfer_excess_funds()
                            self.logger.info(f"基准价已更新: {self.base_price}")

                            # 使用更清晰的格式发送交易成功消息
                            trade_side = "buy" if side == "buy" else "sell"
                            trade_price = float(check_order["price"])
                            trade_amount = float(check_order["filled"])
                            trade_total = trade_price * trade_amount

                            # 交易完成后，检查并转移多余资金到理财
                            await self.position_manager.transfer_excess_funds()

                            return check_order
                    except Exception as check_e:
                        self.logger.error(f"检查订单状态失败: {str(check_e)}")

                # 清除活跃订单状态
                self.active_orders[side] = None

                # 增加重试计数
                retry_count += 1

                # 如果还有重试次数，等待一秒后继续
                if retry_count < max_retries:
                    self.logger.info(f"等待1秒后进行第 {retry_count + 1} 次尝试")
                    await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"执行{side}单失败: {str(e)}")

                # 尝试清理可能存在的订单
                if "order_id" in locals() and self.active_orders.get(side) == order_id:
                    try:
                        await self.exchange.cancel_order(order_id, self.cfg.SYMBOL)
                        self.logger.info(f"已取消错误订单 | ID: {order_id}")
                    except Exception as cancel_e:
                        self.logger.error(f"取消错误订单失败: {str(cancel_e)}")
                    finally:
                        self.active_orders[side] = None

                # 增加重试计数
                retry_count += 1

                # 如果是关键错误，停止重试
                if "资金不足" in str(e) or "Insufficient" in str(e):
                    self.logger.error("资金不足，停止重试")
                    # 发送错误通知
                    error_message = f"""❌ 交易失败
━━━━━━━━━━━━━━━━━━━━
🔍 类型: {side} 失败
📊 交易对: {self.cfg.SYMBOL}
⚠️ 错误: 资金不足
"""
                    return False

                # 如果还有重试次数，稍等后继续
                if retry_count < max_retries:
                    self.logger.info(f"等待2秒后进行第 {retry_count + 1} 次尝试")
                    await asyncio.sleep(2)

        # 达到最大重试次数后仍未成功
        if retry_count >= max_retries:
            self.logger.error(f"{side}单执行失败，达到最大重试次数: {max_retries}")
            error_message = f"""❌ 交易失败
━━━━━━━━━━━━━━━━━━━━
🔍 类型: {side} 失败
📊 交易对: {self.cfg.SYMBOL}
⚠️ 错误: 达到最大重试次数 {max_retries} 次
"""

        return False

    ############################################################################
    # balance checking

    async def check_buy_balance(self):
        """检查买入前的余额，如果不够则从理财赎回"""
        try:
            # 计算所需买入资金 (value in quote currency)
            amount_usdt = await self._calculate_order_amount()

            if await self.position_manager.ensure_trading_funds("BUY", amount_usdt):
                self.logger.info(
                    f"买入资金已确认或准备就绪: {amount_usdt:.2f} {self.position_manager.quote_currency}"
                )
                return True
            else:
                self.logger.error(
                    f"买入资金不足或准备失败: {amount_usdt:.2f} {self.position_manager.quote_currency}"
                )
                return False

        except Exception as e:
            self.logger.error(f"检查买入余额失败: {str(e)}")
            return False

    async def check_sell_balance(self):
        """检查卖出前的余额，如果不够则从理财赎回"""
        try:
            # 计算所需卖出数量 (value in quote currency)
            amount_usdt = await self._calculate_order_amount()

            if await self.position_manager.ensure_trading_funds("SELL", amount_usdt):
                self.logger.info(
                    f"卖出资金已确认或准备就绪 (等值 {amount_usdt:.2f} {self.position_manager.quote_currency})"
                )
                return True
            else:
                self.logger.error(
                    f"卖出资金不足或准备失败 (等值 {amount_usdt:.2f} {self.position_manager.quote_currency})"
                )
                # Optionally send notification here
                return False

        except Exception as e:
            self.logger.error(f"检查卖出余额失败: {str(e)}")
            return False

    ############################################################################
    # signal checking

    # for grid trading logic
    def _get_upper_band(self):
        return self.base_price * (1 + self.grid_size / 100)

    # for grid trading logic
    def _get_lower_band(self):
        return self.base_price * (1 - self.grid_size / 100)

    async def _check_buy_signal(self):
        current_price = self.current_price
        if current_price <= self._get_lower_band():
            self.buying_or_selling = True  # 进入买入或卖出监测
            # 记录最低价
            new_lowest = (
                current_price
                if self.lowest is None
                else min(self.lowest, current_price)
            )
            # 只在最低价更新时打印日志
            if new_lowest != self.lowest:
                self.lowest = new_lowest
                self.logger.info(
                    f"买入监测 | "
                    f"当前价: {current_price:.2f} | "
                    f"触发价: {self._get_lower_band():.5f} | "
                    f"最低价: {self.lowest:.2f} | "
                    f"网格下限: {self._get_lower_band():.2f} | "
                    f"反弹阈值: {TraderGridConfig.flip_threshold(self.grid_size)*100:.2f}%"
                )
            threshold = TraderGridConfig.flip_threshold(self.grid_size)
            # 从最低价反弹指定比例时触发买入
            if self.lowest and current_price >= self.lowest * (1 + threshold):
                self.buying_or_selling = False  # 不在买入或卖出
                self.logger.info(
                    f"触发买入信号 | 当前价: {current_price:.2f} | 已反弹: {(current_price/self.lowest-1)*100:.2f}%"
                )
                # 检查买入余额是否充足
                if not await self.check_buy_balance():
                    return False
                return True
        else:
            self.buying_or_selling = False  # 退出买入或卖出监测
        return False

    async def _check_sell_signal(self):
        current_price = self.current_price
        initial_upper_band = self._get_upper_band()  # 初始上轨价格

        position_ratio = await self.position_manager.get_position_ratio()
        # 使用配置中的开关控制基准价自动修正功能
        if (
            self.cfg.AUTO_ADJUST_BASE_PRICE
            and current_price >= initial_upper_band
            and position_ratio < self.cfg.MIN_POSITION_RATIO
        ):
            # 仓位低于最小仓位，直接修正基准价为当前价格
            old_base_price = self.base_price
            self.base_price = current_price
            self.highest = None  # 重置最高价记录

            # 记录修正日志
            self.logger.info(
                f"基准价修正 | "
                f"原因: 仓位过低 ({position_ratio:.2%} < {self.cfg.MIN_POSITION_RATIO:.2%}) | "
                f"旧基准价: {old_base_price:.2f} | "
                f"新基准价: {current_price:.2f}"
            )

            return False  # 不触发卖出信号

        if current_price >= initial_upper_band:
            self.buying_or_selling = True  # 进入买入或卖出监测
            # 记录最高价
            new_highest = (
                current_price
                if self.highest is None
                else max(self.highest, current_price)
            )
            threshold = TraderGridConfig.flip_threshold(self.grid_size)

            # 计算动态触发价格 (基于最高价的回调阈值)
            dynamic_trigger_price = (
                new_highest * (1 - threshold)
                if new_highest is not None
                else initial_upper_band
            )

            # 只在最高价更新时打印日志
            if new_highest != self.highest:
                self.highest = new_highest
                # 重新计算动态触发价，基于更新后的最高价
                dynamic_trigger_price = self.highest * (1 - threshold)

                self.logger.info(
                    f"卖出监测 | "
                    f"当前价: {current_price:.2f} | "
                    f"触发价(动态): {dynamic_trigger_price:.5f} | "
                    f"最高价: {self.highest:.2f}"
                )

            # 从最高价下跌指定比例时触发卖出
            if self.highest and current_price <= self.highest * (1 - threshold):
                self.buying_or_selling = False  # 不在买入或卖出
                self.logger.info(
                    f"触发卖出信号 | 当前价: {current_price:.2f} | 目标价: {self.highest * (1 - threshold):.5f} | 已下跌: {(1-current_price/self.highest)*100:.2f}%"
                )
                # 检查卖出余额是否充足
                if not await self.check_sell_balance():
                    return False
                return True
        else:
            self.buying_or_selling = False  # 退出买入或卖出监测
        return False

    ############################################################################

    async def _check_signal_with_retry(
        self, check_func, check_name, max_retries=3, retry_delay=2
    ):
        """带重试机制的信号检测函数

        Args:
            check_func: 要执行的检测函数 (_check_buy_signal 或 _check_sell_signal)
            check_name: 检测名称，用于日志
            max_retries: 最大重试次数
            retry_delay: 重试间隔（秒）

        Returns:
            bool: 检测结果
        """
        retries = 0
        while retries <= max_retries:
            try:
                return await check_func()
            except Exception as e:
                retries += 1
                if retries <= max_retries:
                    self.logger.warning(
                        f"{check_name}出错，{retry_delay}秒后进行第{retries}次重试: {str(e)}"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error(
                        f"{check_name}失败，达到最大重试次数({max_retries}次): {str(e)}"
                    )
                    return False
        return False

    async def main_loop(self):
        while True:
            try:
                if not self.initialized:
                    await self.initialize()
                    await self.actioner_s1.update_daily_s1_levels()

                # 保留S1水平更新
                await self.actioner_s1.update_daily_s1_levels()

                # 获取当前价格
                current_price = await self.position_manager.get_latest_price()
                if not current_price:
                    await asyncio.sleep(5)
                    continue
                self.current_price = current_price

                # 优先检查买入卖出信号，不执行风控检查
                # 添加重试机制确保买入卖出检测正常运行
                sell_signal = await self._check_signal_with_retry(
                    self._check_sell_signal, "卖出检测"
                )
                if sell_signal:
                    await self.execute_order("sell")
                else:
                    buy_signal = await self._check_signal_with_retry(
                        self._check_buy_signal, "买入检测"
                    )
                    if buy_signal:
                        await self.execute_order("buy")
                    else:
                        # 只有在没有交易信号时才执行其他操作

                        # 执行风控检查
                        if await self.risk_manager.multi_layer_check():
                            await asyncio.sleep(5)
                            continue

                        # 执行S1策略
                        await self.actioner_s1.check_and_execute()

                        # 如果时间到了并且不在买入或卖出调整网格大小
                        dynamic_interval_seconds = (
                            await self._calculate_dynamic_interval_seconds()
                        )
                        if (
                            time.time() - self.last_grid_adjust_time
                            > dynamic_interval_seconds
                            and not self.buying_or_selling
                        ):
                            self.logger.info(
                                f"时间到了，准备调整网格大小 (间隔: {dynamic_interval_seconds/3600} 小时)."
                            )
                            await self.adjust_grid_size()
                            self.last_grid_adjust_time = time.time()

                await asyncio.sleep(5)

            except Exception as e:
                self.logger.error(f"Main loop error: {e}", exc_info=True)
                await asyncio.sleep(30)

    async def emergency_stop(self):
        try:
            open_orders = await self.exchange.fetch_open_orders(self.cfg.SYMBOL)
            for order in open_orders:
                await self.exchange.cancel_order(
                    order["id"], self.cfg.SYMBOL
                )  # Added symbol to cancel_order
            self.logger.critical("所有交易已停止，进入复盘程序")
        except Exception as e:
            self.logger.error(f"紧急停止失败: {str(e)}")
        finally:
            await self.exchange.close()
            exit()
