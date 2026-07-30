"""
Retry Decorators with Exponential Backoff
==========================================
Provides retry logic for transient failures with exponential backoff and jitter.

Features:
- Exponential backoff with configurable base delay
- Jitter to prevent thundering herd
- Exception filtering (retry only specific exceptions)
- Integration with Syniq logger
- Max retries and timeout support
- Async support for async functions
"""

import time
import random
import asyncio
from typing import Optional, Callable, Tuple, Type, Union, List
from functools import wraps

from utils.logger import get_logger

logger = get_logger()


class RetryError(Exception):
    """Raised when all retry attempts are exhausted"""
    
    def __init__(self, message: str, original_exception: Exception, attempts: int):
        super().__init__(message)
        self.original_exception = original_exception
        self.attempts = attempts


class RetryConfig:
    """Configuration for retry behavior"""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        jitter_range: Tuple[float, float] = (0.0, 1.0),
        retry_on: Optional[Union[Type[Exception], Tuple[Type[Exception], ...]]] = None,
        retry_on_result: Optional[Callable] = None,
        on_retry: Optional[Callable] = None,
        raise_on_failure: bool = True
    ):
        """
        Initialize retry configuration
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay between retries (seconds)
            max_delay: Maximum delay between retries (seconds)
            exponential_base: Base for exponential backoff (delay = base_delay * exponential_base^attempt)
            jitter: Whether to add random jitter to delays
            jitter_range: Range for jitter (0.0 to 1.0 means +/- 100% of calculated delay)
            retry_on: Exception types to retry on (None = retry on all exceptions)
            retry_on_result: Function to determine if result should trigger retry
            on_retry: Callback function called on each retry (receives exception, attempt number)
            raise_on_failure: Whether to raise exception after all retries exhausted
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.jitter_range = jitter_range
        self.retry_on = retry_on or Exception
        self.retry_on_result = retry_on_result
        self.on_retry = on_retry
        self.raise_on_failure = raise_on_failure
    
    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt with exponential backoff and jitter
        
        Args:
            attempt: Attempt number (0-based)
        
        Returns:
            Delay in seconds
        """
        # Exponential backoff
        delay = self.base_delay * (self.exponential_base ** attempt)
        
        # Cap at max_delay
        delay = min(delay, self.max_delay)
        
        # Add jitter
        if self.jitter:
            jitter_min, jitter_max = self.jitter_range
            jitter_factor = random.uniform(jitter_min, jitter_max)
            delay = delay * (1 + jitter_factor)
        
        return delay
    
    def should_retry(self, exception: Exception) -> bool:
        """
        Determine if exception should trigger a retry
        
        Args:
            exception: The exception that was raised
        
        Returns:
            True if should retry, False otherwise
        """
        return isinstance(exception, self.retry_on)


def retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_on: Optional[Union[Type[Exception], Tuple[Type[Exception], ...]]] = None,
    retry_on_result: Optional[Callable] = None,
    on_retry: Optional[Callable] = None,
    raise_on_failure: bool = True
):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries (seconds)
        max_delay: Maximum delay between retries (seconds)
        exponential_base: Base for exponential backoff
        jitter: Whether to add random jitter
        retry_on: Exception types to retry on
        retry_on_result: Function to check if result should trigger retry
        on_retry: Callback on each retry
        raise_on_failure: Whether to raise exception after exhausting retries
    
    Example:
        @retry(max_retries=3, base_delay=2.0, retry_on=(ConnectionError, TimeoutError))
        def fetch_data():
            # Code that might fail transiently
            pass
    """
    config = RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        jitter=jitter,
        retry_on=retry_on or Exception,
        retry_on_result=retry_on_result,
        on_retry=on_retry,
        raise_on_failure=raise_on_failure
    )
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    
                    # Check if result should trigger retry
                    if retry_on_result and retry_on_result(result):
                        if attempt < max_retries:
                            delay = config.calculate_delay(attempt)
                            logger.warning(
                                f"Retry triggered by result | "
                                f"Function: {func.__name__} | "
                                f"Attempt: {attempt + 1}/{max_retries + 1} | "
                                f"Retrying in {delay:.2f}s"
                            )
                            
                            if on_retry:
                                on_retry(None, attempt + 1)
                            
                            time.sleep(delay)
                            continue
                    
                    # Success
                    if attempt > 0:
                        logger.info(f"Retry successful | Function: {func.__name__} | Attempt: {attempt + 1}")
                    
                    return result
                
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry this exception
                    if not config.should_retry(e):
                        logger.error(f"Non-retryable exception in {func.__name__}: {e}")
                        raise
                    
                    # Check if we have retries left
                    if attempt >= max_retries:
                        logger.error(
                            f"Max retries exhausted | "
                            f"Function: {func.__name__} | "
                            f"Attempts: {attempt + 1} | "
                            f"Last error: {e}"
                        )
                        
                        if raise_on_failure:
                            raise RetryError(
                                f"Failed after {attempt + 1} attempts",
                                e,
                                attempt + 1
                            ) from e
                        else:
                            return None
                    
                    # Calculate delay and retry
                    delay = config.calculate_delay(attempt)
                    logger.warning(
                        f"Retryable exception | "
                        f"Function: {func.__name__} | "
                        f"Attempt: {attempt + 1}/{max_retries + 1} | "
                        f"Error: {e} | "
                        f"Retrying in {delay:.2f}s"
                    )
                    
                    if on_retry:
                        on_retry(e, attempt + 1)
                    
                    time.sleep(delay)
            
            # Should not reach here, but just in case
            if raise_on_failure and last_exception:
                raise last_exception
            return None
        
        return wrapper
    
    return decorator


def async_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_on: Optional[Union[Type[Exception], Tuple[Type[Exception], ...]]] = None,
    retry_on_result: Optional[Callable] = None,
    on_retry: Optional[Callable] = None,
    raise_on_failure: bool = True
):
    """
    Async retry decorator with exponential backoff
    
    Args:
        Same as retry() decorator
    
    Example:
        @async_retry(max_retries=3, base_delay=2.0)
        async def fetch_data_async():
            # Async code that might fail
            pass
    """
    config = RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        jitter=jitter,
        retry_on=retry_on or Exception,
        retry_on_result=retry_on_result,
        on_retry=on_retry,
        raise_on_failure=raise_on_failure
    )
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    result = await func(*args, **kwargs)
                    
                    # Check if result should trigger retry
                    if retry_on_result and retry_on_result(result):
                        if attempt < max_retries:
                            delay = config.calculate_delay(attempt)
                            logger.warning(
                                f"Async retry triggered by result | "
                                f"Function: {func.__name__} | "
                                f"Attempt: {attempt + 1}/{max_retries + 1} | "
                                f"Retrying in {delay:.2f}s"
                            )
                            
                            if on_retry:
                                if asyncio.iscoroutinefunction(on_retry):
                                    await on_retry(None, attempt + 1)
                                else:
                                    on_retry(None, attempt + 1)
                            
                            await asyncio.sleep(delay)
                            continue
                    
                    # Success
                    if attempt > 0:
                        logger.info(f"Async retry successful | Function: {func.__name__} | Attempt: {attempt + 1}")
                    
                    return result
                
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry this exception
                    if not config.should_retry(e):
                        logger.error(f"Non-retryable async exception in {func.__name__}: {e}")
                        raise
                    
                    # Check if we have retries left
                    if attempt >= max_retries:
                        logger.error(
                            f"Async max retries exhausted | "
                            f"Function: {func.__name__} | "
                            f"Attempts: {attempt + 1} | "
                            f"Last error: {e}"
                        )
                        
                        if raise_on_failure:
                            raise RetryError(
                                f"Async failed after {attempt + 1} attempts",
                                e,
                                attempt + 1
                            ) from e
                        else:
                            return None
                    
                    # Calculate delay and retry
                    delay = config.calculate_delay(attempt)
                    logger.warning(
                        f"Async retryable exception | "
                        f"Function: {func.__name__} | "
                        f"Attempt: {attempt + 1}/{max_retries + 1} | "
                        f"Error: {e} | "
                        f"Retrying in {delay:.2f}s"
                    )
                    
                    if on_retry:
                        if asyncio.iscoroutinefunction(on_retry):
                            await on_retry(e, attempt + 1)
                        else:
                            on_retry(e, attempt + 1)
                    
                    await asyncio.sleep(delay)
            
            # Should not reach here, but just in case
            if raise_on_failure and last_exception:
                raise last_exception
            return None
        
        return wrapper
    
    return decorator


# Convenience decorators for common scenarios

def retry_on_connection_error(max_retries: int = 3, base_delay: float = 2.0):
    """Retry on connection-related errors"""
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    from sqlalchemy.exc import OperationalError
    
    return retry(
        max_retries=max_retries,
        base_delay=base_delay,
        retry_on=(
            ConnectionError,
            TimeoutError,
            ConnectionFailure,
            ServerSelectionTimeoutError,
            OperationalError
        )
    )


def retry_on_network_error(max_retries: int = 5, base_delay: float = 1.0):
    """Retry on network-related errors"""
    import urllib3.exceptions
    from requests.exceptions import RequestException, ConnectionError as RequestsConnectionError
    
    return retry(
        max_retries=max_retries,
        base_delay=base_delay,
        retry_on=(
            ConnectionError,
            TimeoutError,
            RequestException,
            RequestsConnectionError,
            urllib3.exceptions.HTTPError
        )
    )


def retry_on_s3_error(max_retries: int = 3, base_delay: float = 2.0):
    """Retry on S3/MinIO errors"""
    try:
        from botocore.exceptions import ClientError, BotoCoreError
        retry_exceptions = (ClientError, BotoCoreError, ConnectionError)
    except ImportError:
        retry_exceptions = (ConnectionError,)
    
    return retry(
        max_retries=max_retries,
        base_delay=base_delay,
        retry_on=retry_exceptions
    )


if __name__ == "__main__":
    # Demo usage
    print("\n" + "="*60)
    print("Retry Decorator Demo")
    print("="*60)
    
    # Example 1: Basic retry
    @retry(max_retries=3, base_delay=1.0)
    def flaky_function(fail_count: int = 2):
        """Function that fails a few times then succeeds"""
        if not hasattr(flaky_function, "attempt"):
            flaky_function.attempt = 0
        
        flaky_function.attempt += 1
        print(f"  Attempt {flaky_function.attempt}")
        
        if flaky_function.attempt <= fail_count:
            raise ConnectionError("Network error")
        
        return "Success!"
    
    print("\n1. Basic retry (fails 2 times then succeeds):")
    result = flaky_function(fail_count=2)
    print(f"  Result: {result}")
    flaky_function.attempt = 0  # Reset
    
    # Example 2: Specific exception types
    @retry(max_retries=2, base_delay=0.5, retry_on=(ValueError, KeyError))
    def specific_exceptions():
        """Only retries on ValueError or KeyError"""
        raise ValueError("This will be retried")
    
    print("\n2. Retry on specific exceptions:")
    try:
        specific_exceptions()
    except RetryError as e:
        print(f"  Failed after retries: {e}")
    
    # Example 3: Custom retry callback
    def on_retry_callback(exception, attempt):
        print(f"  Custom callback: Attempt {attempt}, Exception: {exception}")
    
    @retry(max_retries=2, base_delay=0.5, on_retry=on_retry_callback)
    def with_callback():
        raise TimeoutError("Timeout")
    
    print("\n3. Retry with callback:")
    try:
        with_callback()
    except RetryError:
        print("  Failed with callback")
    
    # Example 4: Retry based on result
    @retry(max_retries=3, base_delay=0.5, retry_on_result=lambda x: x is None)
    def retry_on_none():
        """Retries if result is None"""
        if not hasattr(retry_on_none, "attempt"):
            retry_on_none.attempt = 0
        
        retry_on_none.attempt += 1
        
        if retry_on_none.attempt <= 2:
            return None
        return "Success!"
    
    print("\n4. Retry based on result:")
    result = retry_on_none()
    print(f"  Result: {result}")
    
    # Example 5: Exponential backoff visualization
    print("\n5. Exponential backoff delays:")
    config = RetryConfig(base_delay=1.0, exponential_base=2.0, max_delay=30.0, jitter=False)
    for i in range(6):
        delay = config.calculate_delay(i)
        print(f"  Attempt {i + 1}: {delay:.2f}s")
    
    # Example 6: Convenience decorator
    @retry_on_connection_error(max_retries=2, base_delay=0.5)
    def database_operation():
        raise ConnectionError("Database connection lost")
    
    print("\n6. Convenience decorator (retry_on_connection_error):")
    try:
        database_operation()
    except RetryError as e:
        print(f"  Failed after retries: {type(e.original_exception).__name__}")
    
    print("\n" + "="*60)
    print("Demo complete!")
    print("="*60)
