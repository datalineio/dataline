#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import json
import sys
from time import sleep
from typing import Iterable, Sequence

import backoff
import pendulum
from airbyte_cdk.entrypoint import logger  # FIXME (Eugene K): register logger as standard python logger
from facebook_business.exceptions import FacebookRequestError

FACEBOOK_UNKNOWN_ERROR_CODE = 99
FACEBOOK_API_CALL_LIMIT_ERROR_CODES = (4, 17, 32, 613, 8000, 80001, 80002, 80003, 80004, 80005, 80006, 80008)
DEFAULT_SLEEP_INTERVAL = pendulum.Interval(minutes=1)


class FacebookAPIException(Exception):
    """General class for all API errors"""


class JobTimeoutException(Exception):
    """Scheduled job timed out"""


def batch(iterable: Sequence, size: int = 1):
    total_size = len(iterable)
    for ndx in range(0, total_size, size):
        yield iterable[ndx : min(ndx + size, total_size)]


def handle_call_rate_response(exc: FacebookRequestError) -> bool:
    pause_time = DEFAULT_SLEEP_INTERVAL
    platform_header = exc.http_headers().get("x-app-usage") or exc.http_headers().get("x-ad-account-usage")
    if platform_header:
        platform_header = json.loads(platform_header)
        call_count = platform_header.get("call_count") or platform_header.get("acc_id_util_pct")
        if call_count > 99:
            logger.info(f"Reached platform call limit: {exc}")

    buc_header = exc.http_headers().get("x-business-use-case-usage")
    buc_header = json.loads(buc_header) if buc_header else {}
    for business_object_id, stats in buc_header.items():
        if stats["call_count"] > 99:
            logger.info(f"Reached call limit on {stats['type']}: {exc}")
            pause_time = max(pause_time, stats["estimated_time_to_regain_access"])
    logger.info(f"Sleeping for {pause_time.total_seconds()} seconds")
    sleep(pause_time.total_seconds())

    return True


def retry_pattern(backoff_type, exception, **wait_gen_kwargs):
    def log_retry_attempt(details):
        _, exc, _ = sys.exc_info()
        logger.info(str(exc))
        logger.info(f"Caught retryable error after {details['tries']} tries. Waiting {details['wait']} more seconds then retrying...")

    def should_retry_api_error(exc):
        if isinstance(exc, FacebookRequestError):
            if exc.api_error_code() in FACEBOOK_API_CALL_LIMIT_ERROR_CODES:
                return handle_call_rate_response(exc)
            return exc.api_transient_error() or exc.api_error_subcode() == FACEBOOK_UNKNOWN_ERROR_CODE
        else:
            exception_list = exception if isinstance(exception, Iterable) else [exception]
            for exception_type in exception_list:
                if isinstance(exc, exception_type):
                    return True
        return False

    return backoff.on_exception(
        backoff_type,
        exception,
        jitter=None,
        on_backoff=log_retry_attempt,
        giveup=lambda exc: not should_retry_api_error(exc),
        **wait_gen_kwargs,
    )


def deep_merge(a, b):
    """Merge two values, with `b` taking precedence over `a`."""
    if isinstance(a, dict) and isinstance(b, dict):
        # set of all keys in both dictionaries
        keys = set(a.keys()) | set(b.keys())

        return {key: deep_merge(a.get(key), b.get(key)) for key in keys}
    elif isinstance(a, list) and isinstance(b, list):
        return [*a, *b]
    else:
        return a if b is None else b
