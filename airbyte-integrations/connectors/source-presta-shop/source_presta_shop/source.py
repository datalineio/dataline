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


from base64 import b64encode
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

from .streams import (
    Addresses,
    Carriers,
    CartRules,
    Carts,
    Categories,
    Combinations,
    Configurations,
    Contacts,
    ContentManagementSystem,
    Countries,
    Currencies,
    CustomerMessages,
    Customers,
    CustomerThreads,
    Deliveries,
    Employees,
    Groups,
    Guests,
    ImageTypes,
    Languages,
    Manufacturers,
    Messages,
    OrderCarriers,
    OrderDetails,
    OrderHistories,
    OrderInvoices,
    OrderPayments,
    Orders,
    OrderSlip,
    OrderStates,
    PriceRanges,
    ProductCustomizationFields,
    ProductFeatures,
    ProductFeatureValues,
    ProductOptions,
    ProductOptionValues,
    Products,
    ProductSuppliers,
    ShopGroups,
    Shops,
    ShopUrls,
    SpecificPriceRules,
    SpecificPrices,
    States,
    StockAvailables,
    StockMovementReasons,
    StockMovements,
    Stores,
    Suppliers,
    Tags,
    Taxes,
    TaxRuleGroups,
    TaxRules,
    TranslatedConfigurations,
    WeightRanges,
    Zones,
)


class SourcePrestaShop(AbstractSource):
    @staticmethod
    def get_authenticator(config: Mapping[str, Any]):
        token = b64encode(bytes(config["access_key"] + ":", "utf-8")).decode("ascii")
        authenticator = TokenAuthenticator(token, auth_method="Basic")
        return authenticator

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            authenticator = self.get_authenticator(config)
            args = {"authenticator": authenticator, "url": config["url"]}
            shops = Shops(**args).read_records(sync_mode=SyncMode.full_refresh)
            next(shops)
            return True, None
        except Exception as error:
            return False, error

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = self.get_authenticator(config)
        args = {"authenticator": authenticator, "url": config["url"]}
        return [
            Addresses(**args),
            Carriers(**args),
            CartRules(**args),
            Carts(**args),
            Categories(**args),
            Combinations(**args),
            Configurations(**args),
            Contacts(**args),
            ContentManagementSystem(**args),
            Countries(**args),
            Currencies(**args),
            CustomerMessages(**args),
            CustomerThreads(**args),
            Customers(**args),
            Deliveries(**args),
            Employees(**args),
            Groups(**args),
            Guests(**args),
            ImageTypes(**args),
            Languages(**args),
            Manufacturers(**args),
            Messages(**args),
            OrderCarriers(**args),
            OrderDetails(**args),
            OrderHistories(**args),
            OrderInvoices(**args),
            OrderPayments(**args),
            OrderSlip(**args),
            OrderStates(**args),
            Orders(**args),
            PriceRanges(**args),
            ProductCustomizationFields(**args),
            ProductFeatureValues(**args),
            ProductFeatures(**args),
            ProductOptionValues(**args),
            ProductOptions(**args),
            ProductSuppliers(**args),
            Products(**args),
            ShopGroups(**args),
            ShopUrls(**args),
            Shops(**args),
            SpecificPriceRules(**args),
            SpecificPrices(**args),
            States(**args),
            StockAvailables(**args),
            StockMovementReasons(**args),
            StockMovements(**args),
            Stores(**args),
            Suppliers(**args),
            Tags(**args),
            TaxRuleGroups(**args),
            TaxRules(**args),
            Taxes(**args),
            TranslatedConfigurations(**args),
            WeightRanges(**args),
            Zones(**args),
        ]
