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


import os

import pytest
from normalization.destination_type import DestinationType
from normalization.transform_catalog.destination_name_transformer import (
    DestinationNameTransformer,
    strip_accents,
    transform_standard_naming,
)


@pytest.fixture(scope="function", autouse=True)
def before_tests(request):
    # This makes the test run whether it is executed from the tests folder (with pytest/gradle)
    # or from the base-normalization folder (through pycharm)
    unit_tests_dir = os.path.join(request.fspath.dirname, "unit_tests")
    if os.path.exists(unit_tests_dir):
        os.chdir(unit_tests_dir)
    else:
        os.chdir(request.fspath.dirname)
    yield
    os.chdir(request.config.invocation_dir)


@pytest.mark.parametrize(
    "input_str, destination_type, expected",
    [
        # Contains Space character
        ("Hello World", "Postgres", True),
        ("Hello World", "BigQuery", False),
        ("Hello World", "Snowflake", True),
        ("Hello World", "Redshift", True),
        ("Hello World", "Oracle", True),
        # Reserved Word for BigQuery only
        ("Groups", "Postgres", False),
        ("Groups", "BigQuery", True),
        ("Groups", "Snowflake", False),
        ("Groups", "Redshift", False),
        ("Groups", "Oracle", False),
        # Doesnt start with alpha or underscore
        ("100x200", "Postgres", True),
        ("100x200", "BigQuery", False),
        ("100x200", "Snowflake", True),
        ("100x200", "Redshift", True),
        ("100x200", "Oracle", True),
        # Contains non alpha numeric
        ("post.wall", "Postgres", True),
        ("post.wall", "BigQuery", False),
        ("post.wall", "Snowflake", True),
        ("post.wall", "Redshift", True),
        ("post.wall", "Oracle", True),
    ],
)
def test_needs_quote(input_str: str, destination_type: str, expected: bool):
    name_transformer = DestinationNameTransformer(DestinationType.from_string(destination_type))
    assert name_transformer.needs_quotes(input_str) == expected


@pytest.mark.parametrize(
    "input_str, expected",
    [
        ("Hello World!", "Hello World!"),
        ("àêî öÙ", "aei oU"),
    ],
)
def test_strip_accents(input_str: str, expected: str):
    assert strip_accents(input_str) == expected


@pytest.mark.parametrize(
    "expected, input_str",
    [
        ("__identifier_name", "__identifier_name"),
        ("IDENTIFIER_NAME", "IDENTIFIER_NAME"),
        ("123identifier_name", "123identifier_name"),
        ("i0d0e0n0t0i0f0i0e0r0n0a0m0e", "i0d0e0n0t0i0f0i0e0r0n0a0m0e"),
        ("_identifier_name", ",identifier+name"),
        ("identifier_name", "identifiêr name"),
        ("a_unicode_name__", "a_unicode_name_文"),
        ("identifier__name__", "identifier__name__"),
        ("identifier_name_weee", "identifier-name.weee"),
        ("_identifier_name_", '"identifier name"'),
        ("identifier_name", "identifier name"),
        ("identifier_", "identifier%"),
        ("_identifier_", "`identifier`"),
    ],
)
def test_transform_standard_naming(input_str: str, expected: str):
    assert transform_standard_naming(input_str) == expected


@pytest.mark.parametrize(
    "input_str, destination_type, expected, expected_column",
    [
        # Case sensitive names
        ("Identifier Name1", "Postgres", "identifier_name1", "{{ adapter.quote('Identifier Name1') }}"),
        ("Identifier Name2", "BigQuery", "Identifier_Name2", "Identifier_Name2"),
        ("Identifier Name3", "Snowflake", "IDENTIFIER_NAME3", "{{ adapter.quote('Identifier Name3') }}"),
        ("Identifier Name4", "Redshift", "identifier_name4", "{{ adapter.quote('identifier name4') }}"),
        ("Identifier Name4", "Oracle", "IDEN__AME4", "{{ adapter.quote('identifier name4') }}"),
        # Unicode
        ("a-Unicode_name_文1", "Postgres", "a_unicode_name__1", "{{ adapter.quote('a-Unicode_name_文1') }}"),
        ("a-Unicode_name_文2", "BigQuery", "a_Unicode_name__2", "a_Unicode_name__2"),
        ("a-Unicode_name_文3", "Snowflake", "A_UNICODE_NAME__3", "{{ adapter.quote('a-Unicode_name_文3') }}"),
        ("a-Unicode_name_文4", "Redshift", "a_unicode_name__4", "{{ adapter.quote('a-unicode_name_文4') }}"),
        # Doesnt start with alpha or underscore
        ("100x2001", "Postgres", "100x2001", "{{ adapter.quote('100x2001') }}"),
        ("100x2002", "BigQuery", "_100x2002", "_100x2002"),
        ("100x2003", "Snowflake", "100x2003", "{{ adapter.quote('100x2003') }}"),
        ("100x2004", "Redshift", "100x2004", "{{ adapter.quote('100x2004') }}"),
        # Reserved Keywords in BQ
        ("Groups", "Postgres", "groups", "groups"),
        ("Groups", "BigQuery", "Groups", "{{ adapter.quote('Groups') }}"),
        ("Groups", "Snowflake", "GROUPS", "GROUPS"),
        ("Groups", "Redshift", "groups", "groups"),
        # Reserved Keywords
        ("DisTincT", "Postgres", "DisTincT", "{{ adapter.quote('DisTincT') }}"),
        ("DisTincT", "BigQuery", "DisTincT", "{{ adapter.quote('DisTincT') }}"),
        ("DisTincT", "Snowflake", "DisTincT", "{{ adapter.quote('DisTincT') }}"),
        ("DisTincT", "Redshift", "distinct", "{{ adapter.quote('distinct') }}"),
        # Quoted identifiers
        ("'QuoTed1 IdenTifiER'", "Postgres", "_quoted1_identifier_", "{{ adapter.quote('\\'QuoTed1 IdenTifiER\\'') }}"),
        ("'QuoTed2 IdenTifiER'", "BigQuery", "_QuoTed2_IdenTifiER_", "_QuoTed2_IdenTifiER_"),
        ("'QuoTed3 IdenTifiER'", "Snowflake", "_QUOTED3_IDENTIFIER_", "{{ adapter.quote('\\'QuoTed3 IdenTifiER\\'') }}"),
        ("'QuoTed4 IdenTifiER'", "Redshift", "_quoted4_identifier_", "{{ adapter.quote('\\'quoted4 identifier\\'') }}"),
        # Double Quoted identifiers
        ('"QuoTed5 IdenTifiER"', "Postgres", "_quoted5_identifier_", '{{ adapter.quote(\'""QuoTed5 IdenTifiER""\') }}'),
        ('"QuoTed6 IdenTifiER"', "BigQuery", "_QuoTed6_IdenTifiER_", "_QuoTed6_IdenTifiER_"),
        ('"QuoTed7 IdenTifiER"', "Snowflake", "_QUOTED7_IDENTIFIER_", '{{ adapter.quote(\'""QuoTed7 IdenTifiER""\') }}'),
        ('"QuoTed8 IdenTifiER"', "Redshift", "_quoted8_identifier_", '{{ adapter.quote(\'""quoted8 identifier""\') }}'),
    ],
)
def test_normalize_name(input_str: str, destination_type: str, expected: str, expected_column: str):
    t = DestinationType.from_string(destination_type)
    assert DestinationNameTransformer(t).normalize_schema_name(input_str) == expected
    assert DestinationNameTransformer(t).normalize_table_name(input_str) == expected
    assert DestinationNameTransformer(t).normalize_column_name(input_str) == expected_column


@pytest.mark.parametrize(
    "input_str, destination_type, expected, expected_in_jinja",
    [
        # Case sensitive names
        ("Identifier Name", "Postgres", "{{ adapter.quote('Identifier Name') }}", "adapter.quote('Identifier Name')"),
        ("Identifier Name", "BigQuery", "Identifier_Name", "'Identifier_Name'"),
        ("Identifier Name", "Snowflake", "{{ adapter.quote('Identifier Name') }}", "adapter.quote('Identifier Name')"),
        ("Identifier Name", "Redshift", "{{ adapter.quote('identifier name') }}", "adapter.quote('identifier name')"),
        # Reserved Word for BigQuery only
        ("Groups", "Postgres", "groups", "'groups'"),
        ("Groups", "BigQuery", "{{ adapter.quote('Groups') }}", "adapter.quote('Groups')"),
        ("Groups", "Snowflake", "GROUPS", "'GROUPS'"),
        ("Groups", "Redshift", "groups", "'groups'"),
    ],
)
def test_normalize_column_name(input_str: str, destination_type: str, expected: str, expected_in_jinja: str):
    t = DestinationType.from_string(destination_type)
    assert DestinationNameTransformer(t).normalize_column_name(input_str, in_jinja=False) == expected
    assert DestinationNameTransformer(t).normalize_column_name(input_str, in_jinja=True) == expected_in_jinja


@pytest.mark.parametrize(
    "input_str, expected",
    [
        # below the limit
        ("Aaaa_Bbbb_Cccc_Dddd_Eeee_Ffff_Gggg_Hhhh", "Aaaa_Bbbb_Cccc_Dddd_Eeee_Ffff_Gggg_Hhhh"),
        # at the limit
        ("Aaaa_Bbbb_Cccc_Dddd_Eeee_Ffff_Gggg_Hhhh_Iii", "Aaaa_Bbbb_Cccc_Dddd_Eeee_Ffff_Gggg_Hhhh_Iii"),
        # over the limit
        ("Aaaa_Bbbb_Cccc_Dddd_Eeee_Ffff_Gggg_Hhhh_Iiii", "Aaaa_Bbbb_Cccc_Dddd___e_Ffff_Gggg_Hhhh_Iiii"),
        ("Aaaa_Bbbb_Cccc_Dddd_Eeee_a_very_long_name_Ffff_Gggg_Hhhh_Iiii", "Aaaa_Bbbb_Cccc_Dddd___e_Ffff_Gggg_Hhhh_Iiii"),
        ("Aaaa_Bbbb_Cccc_Dddd_Eeee_Ffff_Gggg_Hhhh_Iiii_Jjjj_Kkkk", "Aaaa_Bbbb_Cccc_Dddd___g_Hhhh_Iiii_Jjjj_Kkkk"),
        ("ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz_0123456789", "ABCDEFGHIJKLMNOPQRST__qrstuvwxyz_0123456789"),
    ],
)
def test_truncate_identifier(input_str: str, expected: str):
    """
    Rules about truncations, for example for both of these strings which are too long for the postgres 64 limit:
    - `Aaaa_Bbbb_Cccc_Dddd_Eeee_Ffff_Gggg_Hhhh_Iiii`
    - `Aaaa_Bbbb_Cccc_Dddd_Eeee_a_very_long_name_Ffff_Gggg_Hhhh_Iiii`

    Deciding on how to truncate (in the middle) are being verified in these tests.
    In this instance, both strings ends up as:`Aaaa_Bbbb_Cccc_Dddd___e_Ffff_Gggg_Hhhh_Iiii`
    and can potentially cause a collision in table names.

    Note that dealing with such collisions is not part of `destination_name_transformer` but of the `stream_processor`.
    """
    name_transformer = DestinationNameTransformer(DestinationType.POSTGRES)
    print(f"Truncating from #{len(input_str)} to #{len(expected)}")
    assert name_transformer.truncate_identifier_name(input_str) == expected
