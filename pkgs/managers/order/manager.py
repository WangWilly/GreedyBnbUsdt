import time
from datetime import datetime
import logging
import os
import json

import shutil
import csv

################################################################################


class ManagerOrder:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        self.history_file = os.path.join(self.data_dir, "trade_history.json")
        self.backup_file = os.path.join(self.data_dir, "trade_history.backup.json")
        self.archive_dir = os.path.join(self.data_dir, "archives")
        if not os.path.exists(self.archive_dir):
            os.makedirs(self.archive_dir)
        self.max_archive_months = 12
        self.order_states = {}
        self.trade_count = 0
        self.orders = {}
        self.trade_history = []
        self.load_trade_history()
        self.clean_old_archives()

    def log_order(self, order):
        self.order_states[order["id"]] = {"created": datetime.now(), "status": "open"}

    def add_order(self, order):
        """Add new order to tracker"""
        try:
            order_id = order["id"]
            self.orders[order_id] = {
                "order": order,
                "created_at": datetime.now(),
                "status": order["status"],
                "profit": 0,
            }
            self.trade_count += 1
            self.logger.info(
                f"Order added to tracker | ID: {order_id} | Status: {order['status']}"
            )
        except Exception as e:
            self.logger.error(f"Failed to add order: {str(e)}")
            raise

    def reset(self):
        self.trade_count = 0
        self.orders.clear()
        self.logger.info("Order tracker reset")

    def get_trade_history(self):
        """Get trade history"""
        return self.trade_history

    def load_trade_history(self):
        """Load trade history from file"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, "r", encoding="utf-8") as f:
                    self.trade_history = json.load(f)
                self.logger.info(f"Loaded {len(self.trade_history)} historical trades")
        except Exception as e:
            self.logger.error(f"Failed to load trade history: {str(e)}")

    def save_trade_history(self):
        """Save current trade history to file"""
        try:
            # Backup current file first
            self.backup_history()
            # Save current records
            with open(self.history_file, "w", encoding="utf-8") as f:
                json.dump(self.trade_history, f, ensure_ascii=False, indent=2)
            self.logger.info(
                f"Saved {len(self.trade_history)} trade records to {self.history_file}"
            )
        except Exception as e:
            self.logger.error(f"Failed to save trade history: {str(e)}")

    def backup_history(self):
        """Backup trade history"""
        try:
            if os.path.exists(self.history_file):
                shutil.copy2(self.history_file, self.backup_file)
                self.logger.info("Trade history backup successful")
        except Exception as e:
            self.logger.error(f"Failed to backup trade history: {str(e)}")

    def add_trade(self, trade):
        """Add trade record"""
        # Validate required fields
        required_fields = ["timestamp", "side", "price", "amount", "order_id"]
        for field in required_fields:
            if field not in trade:
                self.logger.error(f"Trade record missing required field: {field}")
                return

        # Validate data types
        try:
            trade["timestamp"] = float(trade["timestamp"])
            trade["price"] = float(trade["price"])
            trade["amount"] = float(trade["amount"])
        except (ValueError, TypeError) as e:
            self.logger.error(f"Trade record data type error: {str(e)}")
            return

        self.logger.info(f"Adding trade record: {trade}")
        self.trade_history.append(trade)
        if len(self.trade_history) > 100:
            self.trade_history = self.trade_history[-100:]
        try:
            # Backup current file first
            self.backup_history()
            with open(self.history_file, "w", encoding="utf-8") as f:
                json.dump(self.trade_history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save trade record: {str(e)}")

    def update_order(self, order_id, status, profit=0):
        if order_id in self.orders:
            self.orders[order_id]["status"] = status
            self.orders[order_id]["profit"] = profit
            if status == "closed":
                # Update order status to closed
                self.logger.info(f"Order closed | ID: {order_id} | Profit: {profit}")

    def get_statistics(self):
        """Get trade statistics"""
        try:
            if not self.trade_history:
                return {
                    "total_trades": 0,
                    "win_rate": 0,
                    "total_profit": 0,
                    "avg_profit": 0,
                    "max_profit": 0,
                    "max_loss": 0,
                    "profit_factor": 0,
                    "consecutive_wins": 0,
                    "consecutive_losses": 0,
                }

            total_trades = len(self.trade_history)
            winning_trades = len([t for t in self.trade_history if t["profit"] > 0])
            total_profit = sum(t["profit"] for t in self.trade_history)
            profits = [t["profit"] for t in self.trade_history]

            # Calculate max consecutive wins and losses
            current_streak = 1
            max_win_streak = 0
            max_loss_streak = 0

            for i in range(1, len(profits)):
                if (profits[i] > 0 and profits[i - 1] > 0) or (
                    profits[i] < 0 and profits[i - 1] < 0
                ):
                    current_streak += 1
                else:
                    if profits[i - 1] > 0:
                        max_win_streak = max(max_win_streak, current_streak)
                    else:
                        max_loss_streak = max(max_loss_streak, current_streak)
                    current_streak = 1

            return {
                "total_trades": total_trades,
                "win_rate": winning_trades / total_trades if total_trades > 0 else 0,
                "total_profit": total_profit,
                "avg_profit": total_profit / total_trades if total_trades > 0 else 0,
                "max_profit": max(profits) if profits else 0,
                "max_loss": min(profits) if profits else 0,
                "profit_factor": (
                    sum(p for p in profits if p > 0)
                    / abs(sum(p for p in profits if p < 0))
                    if sum(p for p in profits if p < 0) != 0
                    else 0
                ),
                "consecutive_wins": max_win_streak,
                "consecutive_losses": max_loss_streak,
            }
        except Exception as e:
            self.logger.error(f"Failed to calculate statistics: {str(e)}")
            return None

    def archive_old_trades(self):
        """Archive old trade records"""
        try:
            if len(self.trade_history) <= 100:
                return

            # Get current month as archive filename
            current_month = datetime.now().strftime("%Y%m")
            archive_file = os.path.join(
                self.archive_dir, f"trades_{current_month}.json"
            )

            # Move old records to archive
            old_trades = self.trade_history[:-100]

            # If archive file exists, read and merge
            if os.path.exists(archive_file):
                with open(archive_file, "r", encoding="utf-8") as f:
                    archived_trades = json.load(f)
                    old_trades = archived_trades + old_trades

            # Save archive
            with open(archive_file, "w", encoding="utf-8") as f:
                json.dump(old_trades, f, ensure_ascii=False, indent=2)

            # Update current trade history
            self.trade_history = self.trade_history[-100:]
            self.logger.info(f"Archived {len(old_trades)} trade records to {archive_file}")
        except Exception as e:
            self.logger.error(f"Failed to archive trade records: {str(e)}")

    def clean_old_archives(self):
        """Clean expired archive files"""
        try:
            archive_files = [
                f for f in os.listdir(self.archive_dir) if f.startswith("trades_")
            ]
            archive_files.sort(reverse=True)  # Sort by time descending

            # Keep only the most recent 12 months of archives
            if len(archive_files) > self.max_archive_months:
                for old_file in archive_files[self.max_archive_months :]:
                    file_path = os.path.join(self.archive_dir, old_file)
                    os.remove(file_path)
                    self.logger.info(f"Deleted expired archive: {old_file}")
        except Exception as e:
            self.logger.error(f"Failed to clean archives: {str(e)}")

    def analyze_trades(self, days=30):
        """Analyze recent trading performance"""
        try:
            if not self.trade_history:
                return None

            # Calculate time range
            now = time.time()
            start_time = now - (days * 24 * 3600)

            # Filter trades within time range
            recent_trades = [
                t for t in self.trade_history if t["timestamp"] > start_time
            ]

            if not recent_trades:
                return None

            # Daily statistics
            daily_stats = {}
            for trade in recent_trades:
                trade_date = datetime.fromtimestamp(trade["timestamp"]).strftime(
                    "%Y-%m-%d"
                )
                if trade_date not in daily_stats:
                    daily_stats[trade_date] = {"trades": 0, "profit": 0, "volume": 0}
                daily_stats[trade_date]["trades"] += 1
                daily_stats[trade_date]["profit"] += trade["profit"]
                daily_stats[trade_date]["volume"] += trade["price"] * trade["amount"]

            return {
                "period": f"Last {days} days",
                "total_days": len(daily_stats),
                "active_days": len(
                    [d for d in daily_stats.values() if d["trades"] > 0]
                ),
                "daily_stats": daily_stats,
                "avg_daily_trades": sum(d["trades"] for d in daily_stats.values())
                / len(daily_stats),
                "avg_daily_profit": sum(d["profit"] for d in daily_stats.values())
                / len(daily_stats),
                "best_day": (
                    max(daily_stats.items(), key=lambda x: x[1]["profit"])
                    if daily_stats
                    else None
                ),
                "worst_day": (
                    min(daily_stats.items(), key=lambda x: x[1]["profit"])
                    if daily_stats
                    else None
                ),
            }
        except Exception as e:
            self.logger.error(f"Failed to analyze trades: {str(e)}")
            return None

    def export_trades(self, format="csv"):
        """Export trade records"""
        try:
            if not self.trade_history:
                return False

            export_dir = os.path.join(self.data_dir, "exports")
            if not os.path.exists(export_dir):
                os.makedirs(export_dir)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if format == "csv":
                export_file = os.path.join(export_dir, f"trades_export_{timestamp}.csv")
                with open(export_file, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(
                        f,
                        fieldnames=[
                            "timestamp",
                            "side",
                            "price",
                            "amount",
                            "profit",
                            "order_id",
                        ],
                    )
                    writer.writeheader()
                    for trade in self.trade_history:
                        writer.writerow(trade)
            else:
                export_file = os.path.join(
                    export_dir, f"trades_export_{timestamp}.json"
                )
                with open(export_file, "w", encoding="utf-8") as f:
                    json.dump(self.trade_history, f, ensure_ascii=False, indent=2)

            self.logger.info(f"Trade records exported to: {export_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to export trade records: {str(e)}")
            return False
