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

from abc import ABC
from datetime import datetime
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream


class PrestaShopStream(HttpStream, ABC):
    """
    PrestaShop API Reference: https://devdocs.prestashop.com/1.7/basics/introduction/
    """

    primary_key = "id"
    data_key = None
    limit = 50

    def __init__(self, url: str, **kwargs):
        super(PrestaShopStream, self).__init__(**kwargs)
        self._url = url
        self._offset = 0
        self._has_next_page = False

    @property
    def url_base(self) -> str:
        return f"{self._url}/api/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return self._has_next_page

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        base_params = {"output_format": "JSON", "display": "full", "limit": ",".join([str(self._offset), str(self.limit)])}
        return base_params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        data_key = self.data_key if self.data_key else self.name
        # pagination can be implemented via limit parameter
        # https://devdocs.prestashop.com/1.7/webservice/tutorials/advanced-use/additional-list-parameters/#limit-parameter
        # when records exist API returns dict, in another case empty list
        if isinstance(response_json, dict):
            records = response_json.get(data_key, [])
            # as API response doesn't contain next_page parameter, we can only check records count to set offset
            record_count = len(records)
            self._has_next_page = record_count != 0
            self._offset += record_count
            yield from records
        else:
            self._has_next_page = False


class IncrementalPrestaShopStream(PrestaShopStream, ABC):
    cursor_field = "date_upd"

    def __init__(self, url: str, **kwargs):
        super(IncrementalPrestaShopStream, self).__init__(url, **kwargs)
        self._end_date = datetime.now()

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_record_state = latest_record.get(self.cursor_field)
        return {self.cursor_field: max(latest_record_state, current_stream_state.get(self.cursor_field, latest_record_state))}

    def request_params(self, stream_state=None, **kwargs):
        stream_state = stream_state or {}
        params = super().request_params(stream_state=stream_state, **kwargs)
        start_date = stream_state.get(self.cursor_field)
        # for filtering interval operator is used
        # https://devdocs.prestashop.com/1.7/webservice/tutorials/advanced-use/additional-list-parameters/#filter-parameter
        if start_date:
            params["filter[date_upd]"] = f"[{start_date},{self._end_date}]"
        params["date"] = 1
        params["sort"] = f"[{self.cursor_field}_ASC]"
        return params


class Addresses(IncrementalPrestaShopStream):
    """
    The Customer, Manufacturer and Customer addresses
    https://devdocs.prestashop.com/1.7/webservice/resources/addresses/
    """

    def path(self, **kwargs) -> str:
        return "addresses"


class Carriers(PrestaShopStream):
    """
    The Carriers that perform deliveries
    https://devdocs.prestashop.com/1.7/webservice/resources/carriers/
    """

    def path(self, **kwargs) -> str:
        return "carriers"


class CartRules(IncrementalPrestaShopStream):
    """
    Cart rules management (discount, promotions, …)
    https://devdocs.prestashop.com/1.7/webservice/resources/cart_rules/
    """

    def path(self, **kwargs) -> str:
        return "cart_rules"


class Carts(IncrementalPrestaShopStream):
    """
    Customer’s carts
    https://devdocs.prestashop.com/1.7/webservice/resources/carts/
    """

    def path(self, **kwargs) -> str:
        return "carts"


class Categories(IncrementalPrestaShopStream):
    """
    The product categories
    https://devdocs.prestashop.com/1.7/webservice/resources/categories/
    """

    def path(self, **kwargs) -> str:
        return "categories"


class Combinations(PrestaShopStream):
    """
    The product combinations
    https://devdocs.prestashop.com/1.7/webservice/resources/combinations/
    """

    def path(self, **kwargs) -> str:
        return "combinations"


class Configurations(PrestaShopStream):
    """
    Shop configuration, used to store miscellaneous parameters from the shop
    (maintenance, multi shop, email settings, …)
    https://devdocs.prestashop.com/1.7/webservice/resources/configurations/
    """

    def path(self, **kwargs) -> str:
        return "configurations"


class Contacts(PrestaShopStream):
    """
    Shop contacts
    https://devdocs.prestashop.com/1.7/webservice/resources/contacts/
    """

    def path(self, **kwargs) -> str:
        return "contacts"


class ContentManagementSystem(PrestaShopStream):
    """
    Content management system
    https://devdocs.prestashop.com/1.7/webservice/resources/content_management_system/
    """

    def path(self, **kwargs) -> str:
        return "content_management_system"


class Countries(PrestaShopStream):
    """
    The countries available on the shop
    https://devdocs.prestashop.com/1.7/webservice/resources/countries/
    """

    def path(self, **kwargs) -> str:
        return "countries"


class Currencies(PrestaShopStream):
    """
    The currencies installed on the shop
    https://devdocs.prestashop.com/1.7/webservice/resources/currencies/
    """

    def path(self, **kwargs) -> str:
        return "currencies"


class CustomerMessages(IncrementalPrestaShopStream):
    """
    Customer services messages
    https://devdocs.prestashop.com/1.7/webservice/resources/customer_messages/
    """

    def path(self, **kwargs) -> str:
        return "customer_messages"


class CustomerThreads(IncrementalPrestaShopStream):
    """
    Customer services threads
    https://devdocs.prestashop.com/1.7/webservice/resources/customer_threads/
    """

    def path(self, **kwargs) -> str:
        return "customer_threads"


class Customers(IncrementalPrestaShopStream):
    """
    The e-shop’s customers
    https://devdocs.prestashop.com/1.7/webservice/resources/customers/
    """

    def path(self, **kwargs) -> str:
        return "customers"


class Deliveries(PrestaShopStream):
    """
    Product deliveries
    https://devdocs.prestashop.com/1.7/webservice/resources/deliveries/
    """

    def path(self, **kwargs) -> str:
        return "deliveries"


class Employees(PrestaShopStream):
    """
    The Employees
    https://devdocs.prestashop.com/1.7/webservice/resources/employees/
    """

    def path(self, **kwargs) -> str:
        return "employees"


class Groups(PrestaShopStream):
    """
    The customer’s groups
    https://devdocs.prestashop.com/1.7/webservice/resources/groups/
    """

    def path(self, **kwargs) -> str:
        return "groups"


class Guests(PrestaShopStream):
    """
    The guests (customers not logged in)
    https://devdocs.prestashop.com/1.7/webservice/resources/guests/
    """

    def path(self, **kwargs) -> str:
        return "guests"


class ImageTypes(PrestaShopStream):
    """
    The image types
    https://devdocs.prestashop.com/1.7/webservice/resources/image_types/
    """

    def path(self, **kwargs) -> str:
        return "image_types"


class Languages(PrestaShopStream):
    """
    Shop languages
    https://devdocs.prestashop.com/1.7/webservice/resources/languages/
    """

    def path(self, **kwargs) -> str:
        return "languages"


class Manufacturers(IncrementalPrestaShopStream):
    """
    The product manufacturers
    https://devdocs.prestashop.com/1.7/webservice/resources/manufacturers/
    """

    def path(self, **kwargs) -> str:
        return "manufacturers"


class Messages(IncrementalPrestaShopStream):
    """
    The customers messages
    https://devdocs.prestashop.com/1.7/webservice/resources/messages/
    """

    cursor_field = "date_add"

    def path(self, **kwargs) -> str:
        return "messages"


class OrderCarriers(IncrementalPrestaShopStream):
    """
    The order carriers
    https://devdocs.prestashop.com/1.7/webservice/resources/order_carriers/
    """

    cursor_field = "date_add"

    def path(self, **kwargs) -> str:
        return "order_carriers"


class OrderDetails(PrestaShopStream):
    """
    The order carriers
    https://devdocs.prestashop.com/1.7/webservice/resources/order_details/
    """

    def path(self, **kwargs) -> str:
        return "order_details"


class OrderHistories(IncrementalPrestaShopStream):
    """
    The Order histories
    https://devdocs.prestashop.com/1.7/webservice/resources/order_histories/
    """

    cursor_field = "date_add"

    def path(self, **kwargs) -> str:
        return "order_histories"


class OrderInvoices(IncrementalPrestaShopStream):
    """
    The Order invoices
    https://devdocs.prestashop.com/1.7/webservice/resources/order_invoices/
    """

    cursor_field = "date_add"

    def path(self, **kwargs) -> str:
        return "order_invoices"


class OrderPayments(IncrementalPrestaShopStream):
    """
    The Order payments
    https://devdocs.prestashop.com/1.7/webservice/resources/order_payments/
    """

    cursor_field = "date_add"

    def path(self, **kwargs) -> str:
        return "order_payments"


class OrderSlip(IncrementalPrestaShopStream):
    """
    The Order slips (used for refund)
    https://devdocs.prestashop.com/1.7/webservice/resources/order_slip/
    """

    data_key = "order_slips"

    def path(self, **kwargs) -> str:
        return "order_slip"


class OrderStates(PrestaShopStream):
    """
    The Order states (Waiting for transfer, Payment accepted, …)
    https://devdocs.prestashop.com/1.7/webservice/resources/order_states/
    """

    def path(self, **kwargs) -> str:
        return "order_states"


class Orders(IncrementalPrestaShopStream):
    """
    The Customers orders
    https://devdocs.prestashop.com/1.7/webservice/resources/orders/
    """

    def path(self, **kwargs) -> str:
        return "orders"


class PriceRanges(PrestaShopStream):
    """
    Price range
    https://devdocs.prestashop.com/1.7/webservice/resources/price_ranges/
    """

    def path(self, **kwargs) -> str:
        return "price_ranges"


class ProductCustomizationFields(PrestaShopStream):
    """
    The Product customization fields
    https://devdocs.prestashop.com/1.7/webservice/resources/product_customization_fields/
    """

    data_key = "customization_fields"

    def path(self, **kwargs) -> str:
        return "product_customization_fields"


class ProductFeatureValues(PrestaShopStream):
    """
    The product feature values (Ceramic, Polyester, … - Removable cover, Short sleeves, …)
    https://devdocs.prestashop.com/1.7/webservice/resources/product_feature_values/
    """

    def path(self, **kwargs) -> str:
        return "product_feature_values"


class ProductFeatures(PrestaShopStream):
    """
    The product features (Composition, Property, …)
    https://devdocs.prestashop.com/1.7/webservice/resources/product_features/
    """

    def path(self, **kwargs) -> str:
        return "product_features"


class ProductOptionValues(PrestaShopStream):
    """
    The product options value (S, M, L, … - White, Camel, …)
    https://devdocs.prestashop.com/1.7/webservice/resources/product_option_values/
    """

    def path(self, **kwargs) -> str:
        return "product_option_values"


class ProductOptions(PrestaShopStream):
    """
    The product options (Size, Color, …)
    https://devdocs.prestashop.com/1.7/webservice/resources/product_options/
    """

    def path(self, **kwargs) -> str:
        return "product_options"


class ProductSuppliers(PrestaShopStream):
    """
    Product Suppliers
    https://devdocs.prestashop.com/1.7/webservice/resources/product_suppliers/
    """

    def path(self, **kwargs) -> str:
        return "product_suppliers"


class Products(IncrementalPrestaShopStream):
    """
    The products
    https://devdocs.prestashop.com/1.7/webservice/resources/products/
    """

    def path(self, **kwargs) -> str:
        return "products"


class ShopGroups(PrestaShopStream):
    """
    Shop groups from multi-shop feature
    https://devdocs.prestashop.com/1.7/webservice/resources/shop_groups/
    """

    def path(self, **kwargs) -> str:
        return "shop_groups"


class ShopUrls(PrestaShopStream):
    """
    Shop urls from multi-shop feature
    https://devdocs.prestashop.com/1.7/webservice/resources/shop_urls/
    """

    def path(self, **kwargs) -> str:
        return "shop_urls"


class Shops(PrestaShopStream):
    """
    Shops from multi-shop feature
    https://devdocs.prestashop.com/1.7/webservice/resources/shops/
    """

    def path(self, **kwargs) -> str:
        return "shops"


class SpecificPriceRules(PrestaShopStream):
    """
    Specific price rules management
    https://devdocs.prestashop.com/1.7/webservice/resources/specific_price_rules/
    """

    def path(self, **kwargs) -> str:
        return "specific_price_rules"


class SpecificPrices(PrestaShopStream):
    """
    Specific price management
    https://devdocs.prestashop.com/1.7/webservice/resources/specific_prices/
    """

    def path(self, **kwargs) -> str:
        return "specific_prices"


class States(PrestaShopStream):
    """
    The available states of countries
    https://devdocs.prestashop.com/1.7/webservice/resources/states/
    """

    def path(self, **kwargs) -> str:
        return "states"


class StockAvailables(PrestaShopStream):
    """
    Available quantities of products
    https://devdocs.prestashop.com/1.7/webservice/resources/stock_availables/
    """

    def path(self, **kwargs) -> str:
        return "stock_availables"


class StockMovementReasons(IncrementalPrestaShopStream):
    """
    The stock movement reason (Increase, Decrease, Custom Order, …)
    https://devdocs.prestashop.com/1.7/webservice/resources/stock_movement_reasons/
    """

    def path(self, **kwargs) -> str:
        return "stock_movement_reasons"


class StockMovements(IncrementalPrestaShopStream):
    """
    Stock movements management
    https://devdocs.prestashop.com/1.7/webservice/resources/stock_movements/
    """

    data_key = "stock_mvts"
    cursor_field = "date_add"

    def path(self, **kwargs) -> str:
        return "stock_movements"


class Stores(IncrementalPrestaShopStream):
    """
    The stores
    https://devdocs.prestashop.com/1.7/webservice/resources/stores/
    """

    def path(self, **kwargs) -> str:
        return "stores"


class Suppliers(IncrementalPrestaShopStream):
    """
    The product suppliers
    https://devdocs.prestashop.com/1.7/webservice/resources/suppliers/
    """

    def path(self, **kwargs) -> str:
        return "suppliers"


class Tags(PrestaShopStream):
    """
    The Products tags
    https://devdocs.prestashop.com/1.7/webservice/resources/tags/
    """

    def path(self, **kwargs) -> str:
        return "tags"


class TaxRuleGroups(IncrementalPrestaShopStream):
    """
    Group of Tax rule, along with their name
    https://devdocs.prestashop.com/1.7/webservice/resources/tax_rule_groups/
    """

    def path(self, **kwargs) -> str:
        return "tax_rule_groups"


class TaxRules(PrestaShopStream):
    """
    Tax rules, to associate Tax with a country, zip code, …
    https://devdocs.prestashop.com/1.7/webservice/resources/tax_rules/
    """

    def path(self, **kwargs) -> str:
        return "tax_rules"


class Taxes(PrestaShopStream):
    """
    The tax rate
    https://devdocs.prestashop.com/1.7/webservice/resources/taxes/
    """

    def path(self, **kwargs) -> str:
        return "taxes"


class TranslatedConfigurations(PrestaShopStream):
    """
    Shop configuration which are translated
    https://devdocs.prestashop.com/1.7/webservice/resources/translated_configurations/
    """

    # This API endpoint has cursor field date_upd, but it has empty value

    def path(self, **kwargs) -> str:
        return "translated_configurations"


class WeightRanges(PrestaShopStream):
    """
    Weight ranges for deliveries
    https://devdocs.prestashop.com/1.7/webservice/resources/weight_ranges/
    """

    def path(self, **kwargs) -> str:
        return "weight_ranges"


class Zones(PrestaShopStream):
    """
    The Countries zones
    https://devdocs.prestashop.com/1.7/webservice/resources/zones/
    """

    def path(self, **kwargs) -> str:
        return "zones"
