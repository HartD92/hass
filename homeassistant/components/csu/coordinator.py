"""Coordinator for CSU data."""

from datetime import datetime, timedelta
import logging
from types import MappingProxyType
from typing import Any, cast

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, UnitOfEnergy, UnitOfVolume
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import aiohttp_client
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .csu import CSU, AggregateType, Meter, MeterType, ReadResolution, UsageRead
from .exceptions import InvalidAuth

_LOGGER = logging.getLogger(__name__)


class CsuCoordinator(DataUpdateCoordinator[dict[str, UsageRead]]):
    """Handle fetching CSU data, updating sensors and inserting statistics."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry_data: MappingProxyType[str, Any],
    ) -> None:
        """Initialize the data handler."""
        super().__init__(
            hass,
            _LOGGER,
            name="Csu",
            update_interval=timedelta(hours=12),
        )
        self.api = CSU(
            aiohttp_client.async_get_clientsession(hass),
            entry_data[CONF_USERNAME],
            entry_data[CONF_PASSWORD],
        )

        @callback
        def _dummy_listener() -> None:
            pass

        self.async_add_listener(_dummy_listener)

    async def _async_update_data(self):
        """Fetch data from CSU."""
        try:
            await self.api.async_login()
        except InvalidAuth as err:
            raise ConfigEntryAuthFailed from err
        # I think we need to write another function for this. Usage Reads takes a meter, and we need to get the meters first.
        # we're trying to get the usage for the current bill for each meter, the pass that along.
        #usage_reads: list[UsageRead] = await self.api.async_get_usage_reads()
        #_LOGGER.debug("Updating sensor data with: %s", usage_reads)
        await self._insert_statistics()

    async def _insert_statistics(self) -> None:
        """Insert CSU Statistics."""
        for meter in await self.api.async_get_meters():
            id_prefix = f"CSU_{meter.meter_type.name.lower()}_{meter.customer}"

            consumption_statistic_id = f"{DOMAIN}:{id_prefix}_energy_consumption"
            _LOGGER.debug(
                "Updating Statistics for %s",
                consumption_statistic_id,
            )

            last_stat = await get_instance(self.hass).async_add_executor_job(
                get_last_statistics, self.hass, 1, consumption_statistic_id, True, set()
            )
            if not last_stat:
                _LOGGER.debug("Updating statistics for the first time")
                usage_reads = await self._async_get_usage_reads(
                    meter, self.api.utility.timezone()
                )
                consumption_sum = 0.0
                last_stats_time = None
            else:
                usage_reads = await self._async_get_usage_reads(
                    meter,
                    self.api.utility.timezone(),
                    last_stat[consumption_statistic_id][0]["start"],
                )
                if not usage_reads:
                    _LOGGER.debug("No recent usage data. Skipping update")
                    continue
                stats = await get_instance(self.hass).async_add_executor_job(
                    statistics_during_period,
                    self.hass,
                    usage_reads[0].start_time,
                    None,
                    {consumption_statistic_id},
                    "hour" if meter.meter_type == MeterType.ELEC else "day",
                    None,
                    {"sum"},
                )
                consumption_sum = cast(float, stats[consumption_statistic_id][0]["sum"])
                last_stats_time = stats[consumption_statistic_id][0]["start"]

            consumption_statistics = []

            for usage_read in usage_reads:
                start = usage_read.start_time
                if last_stats_time is not None and start.timestamp() <= last_stats_time:
                    continue
                consumption_sum += usage_read.consumption

                consumption_statistics.append(
                    StatisticData(
                        start=start, state=usage_read.consumption, sum=consumption_sum
                    )
                )

            name_prefix = f"CSU {meter.meter_type.name.lower()} {meter.customer}"

            consumption_metadata = StatisticMetaData(
                has_mean=False,
                has_sum=True,
                name=f"{name_prefix} consumption",
                source=DOMAIN,
                statistic_id=consumption_statistic_id,
                unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR
                if meter.meter_type == MeterType.ELEC
                else UnitOfVolume.CENTUM_CUBIC_FEET,
            )

            async_add_external_statistics(
                self.hass, consumption_metadata, consumption_statistics
            )

    async def _async_get_usage_reads(
        self, meter: Meter, time_zone_str: str, start_time: float | None = None
    ) -> list[UsageRead]:
        """Get usage reads.

        If start_time is None, get usage reads since account activation, otherwise since start_time - 30 days to allow corrections in data from utilities.

        We read at different resolutions depending on age:
        - day resolution for past 3 years
        - hour resolution for past 2 months
        - maybe quarter hour for elec?
        """

        def _update_with_finer_usage_reads(
            usage_reads: list[UsageRead], finer_usage_reads: list[UsageRead]
        ) -> None:
            for i, usage_read in enumerate(usage_reads):
                for j, finer_usage_read in enumerate(finer_usage_reads):
                    if usage_read.start_time == finer_usage_read.start_time:
                        usage_reads[i:] = finer_usage_reads[j:]
                        return
                    if usage_read.end_time == finer_usage_read.start_time:
                        usage_reads[i + 1 :] = finer_usage_reads[j:]
                        return
                    if usage_read.end_time < finer_usage_read.start_time:
                        break
            usage_reads += finer_usage_reads

        tz = await dt_util.async_get_time_zone(time_zone_str)
        if start_time is None:
            start = None
        else:
            start = datetime.fromtimestamp(start_time, tz=tz) - timedelta(days=30)
        end = dt_util.now(tz)
        usage_reads = await self.api.async_get_usage_reads(
            meter, AggregateType.DAY, start, end
        )
        if meter.read_resolution == ReadResolution.DAY:
            return usage_reads

        if start_time is None:
            start = end - timedelta(days=2 * 30)
        else:
            assert start
            start = max(start, end - timedelta(days=2 * 30))
        hourly_usage_reads = await self.api.async_get_usage_reads(
            meter, AggregateType.HOUR, start, end
        )
        _update_with_finer_usage_reads(usage_reads, hourly_usage_reads)
        return usage_reads

        # add in additional update for quarter-hour reads?
