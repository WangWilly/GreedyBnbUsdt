import asyncio
import traceback

from pkgs.clients.exchange import ExchangeClient, ExchangeClientConfig
from pkgs.traders.grid.trader import TraderGrid, TraderGridConfig
from pkgs.utils.logging import get_logger_named, set_default_level

from pkgs.utils.webserver import start_web_server

################################################################################

async def main():
    set_default_level(True)
    logger = get_logger_named("main")
    logger.info("Service is starting...")

    ############################################################################

    try:
        exchange_cfg = ExchangeClientConfig()
        exchange = ExchangeClient(exchange_cfg)
    except Exception as e:
        error_msg = f"Exchange client initialization failed: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        return

    try:
        trader_cfg = TraderGridConfig()
        trader = TraderGrid(trader_cfg, exchange)
        await trader.initialize()
        
        web_server_task = asyncio.create_task(start_web_server(trader))
        trading_task = asyncio.create_task(trader.main_loop())
        
        await asyncio.gather(web_server_task, trading_task)
        
    except Exception as e:
        error_msg = f"启动失败: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        
    finally:
        if 'trader' in locals():
            try:
                await trader.exchange.close()
                logger.info("交易所连接已关闭")
            except Exception as e:
                logger.error(f"关闭连接时发生错误: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main()) 
