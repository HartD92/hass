import dataclasses
from datetime import datetime
from enum import Enum
import json
from typing import Any, Optional

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
import arrow

from .const import TIME_ZONE, USER_AGENT
from .exceptions import CannotConnect, InvalidAuth


@dataclasses.dataclass
class Customer:
    """Data about a customer."""

    customerId: str
    customerContext: dict


class AggregateType(Enum):
    """How to aggregate historical data."""

    BILL = "bill"
    DAY = "day"
    HOUR = "hour"
    QUARTER = "quarter"

    def __str__(self):
        """Return the value of the enum."""
        return self.value


class MeterType(Enum):
    """Meter type. Electric, gas, or water."""

    ELEC = "ELEC"
    GAS = "GAS"
    WATER = "WATER"

    def __str__(self):
        """Return the value of the enum."""
        return self.value

class ReadResolution(Enum):
    """Minimum supported resolution."""

    BILLING = "BILLING"
    DAY = "DAY"
    HOUR = "HOUR"
    QUARTER_HOUR = "QUARTER_HOUR"

    def __str__(self):
        """Return the value of the enum."""
        return self.value


SUPPORTED_AGGREGATE_TYPES = {
    ReadResolution.BILLING: [AggregateType.BILL],
    ReadResolution.DAY: [AggregateType.BILL, AggregateType.DAY],
    ReadResolution.HOUR: [AggregateType.BILL, AggregateType.DAY, AggregateType.HOUR],
    ReadResolution.QUARTER_HOUR: [
        AggregateType.BILL,
        AggregateType.DAY,
        AggregateType.HOUR,
    ],
}


@dataclasses.dataclass
class Meter:
    """Data about a Meter."""

    customer: Customer
    meterNumber: int
    serviceNumber: str
    serviceId: str
    contractNum: str
    meter_type: MeterType
    read_resolution: Optional[ReadResolution]
    

@dataclasses.dataclass
class UsageRead:
    """A read from the meeter that has consumption data."""
    meter: Meter
    start_time: datetime
    end_time: datetime
    consumption: float


@dataclasses.dataclass
class CostRead:
    """A read from the meter that has both consumption and cost data."""

    start_time: datetime
    end_time: datetime
    consumption: float  # taken from value field, in KWH or CCF
    provided_cost: float  # in $


class CSU:
    """Class that can get historical usage/cost from Colorado Springs Utilities."""

    def __init__(
        self, session: aiohttp.ClientSession, username: str, password: str
    ) -> None:
        self.session = session
        self.username = username
        self.password = password
        self.access_token = ""
        self.customers = [Customer]
        self.meters = [Meter]

    async def async_login(self) -> None:
        customerId = ""
        data = aiohttp.FormData()
        data.add_field("username", self.username)
        data.add_field("password", self.password)
        data.add_field("mfa", "")
        data.add_field("grant_type", "password")
        try:
            async with self.session.post(
                "https://myaccount.csu.org/rest/oauth/token",
                data=data,
                headers={
                    "User-Agent": USER_AGENT,
                    "Authorization": "Basic d2ViQ2xpZW50SWRQYXNzd29yZDpzZWNyZXQ=",
                },
                raise_for_status=True,
            ) as resp:
                result = await resp.json()
                if "errorMsg" in result:
                    raise InvalidAuth(result["errorMsg"])
                else:
                    self.access_token = result["access_token"]
                    customerId = result["user"]["customerId"]
                    # self.customers.append(Customer(customerId=result['user']["customerId"]))
                    print(self.access_token)

        except ClientResponseError as err:
            if err.status in (401, 403):
                raise InvalidAuth(err)
            else:
                raise CannotConnect(err)

        try:
            async with self.session.post(
                "https://myaccount.csu.org/rest/account/list/",
                data=json.dumps({"customerId": customerId}),
                headers={
                    "User-Agent": USER_AGENT,
                    "Authorization": "Bearer " + self.access_token,
                    "Content-Type": "application/json",
                },
            ) as resp:
                result = await resp.json()
                if "errorMsg" in result:
                    raise InvalidAuth(result["errorMsg"])
                else:
                    customerContext = result["account"][0]
                    self.customers.append(
                        Customer(customerId=customerId, customerContext=customerContext)
                    )
                    print(self.customers)
        except ClientResponseError as err:
            if err.status in (401, 403):
                raise InvalidAuth(err)
            else:
                raise CannotConnect(err)

    async def async_get_meters(self) -> None:
        try:
            async with self.session.post(
                "https://myaccount.csu.org/rest/account/services/",
                data=json.dumps(
                    {
                        "customerId": self.customers[0].customerId,
                        "multiAcctLimit": 10,
                        "accountContext": self.customers[0].customerContext,
                    }
                ),
                headers={
                    "User-Agent": USER_AGENT,
                    "Authorization": "Bearer " + self.access_token,
                    "Content-Type": "application/json",
                },
            ) as resp:
                result = await resp.json()
                if "errorMsg" in result:
                    raise InvalidAuth(result["errorMsg"])
                else:
                    meters = result["accountSummaryType"]["servicesForGraph"]
                    for meter in meters:
                        if meter["serviceNumber"] == "G-TYPICAL":
                            meterType = MeterType.GAS
                            readFrequency = ReadResolution.DAY
                        elif meter["serviceNumber"] == "W-TYPICAL":
                            meterType = MeterType.WATER
                            readFrequency = ReadResolution.DAY
                        elif meter["serviceNumber"] == "E-TYPICAL":
                            meterType = MeterType.ELEC
                            readFrequency = ReadResolution.QUARTER_HOUR
                        self.meters.append(
                            Meter(
                                customer=self.customers[0],
                                meterNumber=meter["meterNumber"],
                                serviceNumber=meter["serviceNumber"],
                                serviceId=meter["serviceId"],
                                contractNum=meter["serviceContract"],
                                meter_type=meterType,
                                read_resolution=readFrequency,
                            )
                        )
                    print(self.meters)
        except ClientResponseError as err:
            if err.status in (401, 403):
                raise InvalidAuth(err)
            else:
                raise CannotConnect(err)

    async def async_get_usage_reads(
        self,
        meter: Meter,
        aggregate_type: AggregateType,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[UsageRead]:
        """Get usage reads for a meter."""
        if not meter.read_resolution:
            raise ValueError("Meter does not have a read resolution")
        if aggregate_type not in SUPPORTED_AGGREGATE_TYPES[meter.read_resolution]:
            raise ValueError(f"Unsupported aggregate type for {meter.read_resolution}")
        if not start_date:
            raise ValueError("start_date is required")
        if not end_date:
            raise ValueError("end_date is required")
        if start_date > end_date:
            raise ValueError("start_date must be before end_date")

        reads = await self.async_get_dated_data(
            meter=meter,
            url="https://myaccount.csu.org/rest/usage/detail/",
            aggregate_type=aggregate_type,
            start_date=start_date,
            end_date=end_date,
        )
        if (
            aggregate_type == AggregateType.QUARTER
            or aggregate_type == AggregateType.HOUR
        ):
            meterReadField = "readDateTime"
            consumptionField = "scaledRead"
        else:
            meterReadField = "readDate"
            consumptionField = "usageConsumptionValue"

        result = []
        readStart = start_date
        for read in reads:
            if read[meterReadField] is not None:
                result.append(
                    UsageRead(
                        # can perhaps just take readDateTime and subtract read["intervalType"]?
                        meter=meter,
                        start_time=readStart,
                        end_time=datetime.fromisoformat(read[meterReadField]),
                        consumption=read[consumptionField],
                    )
                )
                readStart = datetime.fromisoformat(read[meterReadField])
        return result

    async def async_get_dated_data(
        self,
        meter: Meter,
        url: str,
        aggregate_type: AggregateType,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[Any]:
        """Wrapper for _async_fetch to break large requests into smaller batches."""
        if start_date is None:
            raise ValueError("start_date is required")
        if end_date is None:
            raise ValueError("end_date is required")

        start = arrow.get(start_date.date(), TIME_ZONE)
        end = arrow.get(end_date.date(), TIME_ZONE)

        max_request_days = 30
        if aggregate_type == AggregateType.DAY:
            max_request_days = 6
            url_end = "month"
        if (
            aggregate_type == AggregateType.QUARTER
            or aggregate_type == AggregateType.HOUR
        ):
            max_request_days = 1
            url_end = "day"

        url = url + url_end
        print(url)
        result: list[Any] = []
        req_end = end
        while True:
            req_start = start
            if max_request_days is not None:
                req_start = max(start, req_end.shift(days=-max_request_days))
            if req_start >= req_end:
                return result
            reads = await self._async_fetch(meter, url, req_start, req_end)
            if not reads:
                return result
            result = reads + result
            # req_end = req_start.shift(days=-1)
            req_end = req_end.shift(days=-max_request_days)

    async def _async_fetch(
        self,
        meter: Meter,
        url: str,
        start_date: datetime | arrow.Arrow | None = None,
        end_date: datetime | arrow.Arrow | None = None,
    ) -> list[Any]:
        data = {
            "customerId": meter.customer.customerId,
            "meterNumber": meter.meterNumber,
            "serviceNumber": meter.serviceNumber,
            "serviceId": meter.serviceId,
            "accountContext": meter.customer.customerContext,
            "contractNum": meter.contractNum,
        }
        headers = self._get_headers()
        headers["Content-Type"] = "application/json"

        if start_date:
            data["fromDate"] = start_date.date().strftime("%Y-%m-%d %H:%M")
        if end_date:
            data["toDate"] = end_date.date().strftime("%Y-%m-%d %H:%M")
        try:
            async with self.session.post(
                url, data=json.dumps(data), headers=headers, raise_for_status=True
            ) as resp:
                result = await resp.json()
                # if DEBUG_LOG_RESPONSE:
                #   _LOGGER.debug("Fetched: %s", json.dumps(result,indent=2))
                return result["history"]
        except ClientResponseError as err:
            # Ignore server errors for BILL requests
            # that can happen if end_date is before account activation
            if err.status == 500:
                return []
            raise err

    def _get_headers(self):
        headers = {"User-Agent": USER_AGENT}
        if self.access_token:
            headers["authorization"] = f"Bearer {self.access_token}"
        return headers
