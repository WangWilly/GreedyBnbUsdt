import functools
import logging
from typing import Callable, Tuple, TypeVar, Any, Optional, Awaitable, cast, overload
import asyncio

################################################################################

# Define type variables for generic typing
T = TypeVar('T')  # Return type for functions
E = TypeVar('E', bound=Exception)  # Exception type

################################################################################

def try_execute(logger: Optional[logging.Logger] = None) -> Callable:
    """
    Decorator that converts exception-based code into Go-like error handling.
    
    Usage:
        @try_execute(logger)
        async def my_function():
            # Code that might raise exceptions
            return result
        
        result, err = await my_function()
        if err:
            # Handle error
    """
    def decorator(func: Callable[..., T]) -> Callable[..., Tuple[Any, Optional[Exception]]]:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Tuple[Any, Optional[Exception]]:
            try:
                result = await func(*args, **kwargs)
                return result, None
            except Exception as e:
                if logger:
                    logger.error(f"Error in {func.__name__}: {str(e)}")
                return None, e
                
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> Tuple[Any, Optional[Exception]]:
            try:
                result = func(*args, **kwargs)
                return result, None
            except Exception as e:
                if logger:
                    logger.error(f"Error in {func.__name__}: {str(e)}")
                return None, e
        
        # Choose the appropriate wrapper based on whether the function is async
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator

# Helper for direct function execution (not as decorator)
async def try_async(
    logger, 
    func: Callable[..., Awaitable[T]], 
    *args, 
    **kwargs
) -> Tuple[T, None] | Tuple[None, Exception]:
    """
    Execute an async function and return (result, error) tuple.
    Uses type inference for better IDE and static analysis support.
    
    Args:
        logger: Logger instance to log errors (inferred type)
        func: Async function to execute (return type will be preserved)
        *args: Positional arguments to pass to func
        **kwargs: Keyword arguments to pass to func
    
    Returns:
        A tuple containing either (result, None) or (None, exception)
    """
    try:
        result = await func(*args, **kwargs)
        return result, None
    except Exception as e:
        if logger:
            logger.error(f"Error in {func.__name__}: {str(e)}")
        return None, e

def try_sync(logger: Optional[logging.Logger], func: Callable, *args, **kwargs) -> Tuple[Any, Optional[Exception]]:
    """Execute a sync function and return (result, error) tuple"""
    try:
        result = func(*args, **kwargs)
        return result, None
    except Exception as e:
        if logger:
            logger.error(f"Error in {func.__name__}: {str(e)}")
        return None, e
