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

from datetime import date, timedelta

from airbyte_cdk.sources.streams.http.auth import NoAuth
from source_mixpanel.source import Annotations


def test_date_slices():

    now = date.today()
    # Test with start_date now range
    stream_slices = Annotations(authenticator=NoAuth(), start_date=now, end_date=now, date_window_size=1).stream_slices(sync_mode="any")
    assert 1 == len(stream_slices)

    stream_slices = Annotations(authenticator=NoAuth(), start_date=now - timedelta(days=1), end_date=now, date_window_size=1).stream_slices(
        sync_mode="any"
    )
    assert 2 == len(stream_slices)

    stream_slices = Annotations(authenticator=NoAuth(), start_date=now - timedelta(days=2), end_date=now, date_window_size=1).stream_slices(
        sync_mode="any"
    )
    assert 3 == len(stream_slices)

    stream_slices = Annotations(
        authenticator=NoAuth(), start_date=now - timedelta(days=2), end_date=now, date_window_size=10
    ).stream_slices(sync_mode="any")
    assert 1 == len(stream_slices)

    # test with attribution_window
    stream_slices = Annotations(
        authenticator=NoAuth(), start_date=now - timedelta(days=2), end_date=now, date_window_size=1, attribution_window=5
    ).stream_slices(sync_mode="any")
    assert 8 == len(stream_slices)

    # Test with start_date end_date range
    stream_slices = Annotations(
        authenticator=NoAuth(), start_date=date.fromisoformat("2021-07-01"), end_date=date.fromisoformat("2021-07-01"), date_window_size=1
    ).stream_slices(sync_mode="any")
    assert [{"start_date": "2021-07-01", "end_date": "2021-07-01"}] == stream_slices

    stream_slices = Annotations(
        authenticator=NoAuth(), start_date=date.fromisoformat("2021-07-01"), end_date=date.fromisoformat("2021-07-02"), date_window_size=1
    ).stream_slices(sync_mode="any")
    assert [{"start_date": "2021-07-01", "end_date": "2021-07-01"}, {"start_date": "2021-07-02", "end_date": "2021-07-02"}] == stream_slices

    stream_slices = Annotations(
        authenticator=NoAuth(), start_date=date.fromisoformat("2021-07-01"), end_date=date.fromisoformat("2021-07-03"), date_window_size=1
    ).stream_slices(sync_mode="any")
    assert [
        {"start_date": "2021-07-01", "end_date": "2021-07-01"},
        {"start_date": "2021-07-02", "end_date": "2021-07-02"},
        {"start_date": "2021-07-03", "end_date": "2021-07-03"},
    ] == stream_slices

    stream_slices = Annotations(
        authenticator=NoAuth(), start_date=date.fromisoformat("2021-07-01"), end_date=date.fromisoformat("2021-07-03"), date_window_size=2
    ).stream_slices(sync_mode="any")
    assert [{"start_date": "2021-07-01", "end_date": "2021-07-02"}, {"start_date": "2021-07-03", "end_date": "2021-07-03"}] == stream_slices

    # test with stream_state
    stream_slices = Annotations(
        authenticator=NoAuth(), start_date=date.fromisoformat("2021-07-01"), end_date=date.fromisoformat("2021-07-03"), date_window_size=1
    ).stream_slices(sync_mode="any", stream_state={"date": "2021-07-02"})
    assert [{"start_date": "2021-07-02", "end_date": "2021-07-02"}, {"start_date": "2021-07-03", "end_date": "2021-07-03"}] == stream_slices
