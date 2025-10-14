#!/usr/bin/env python3
"""
MAVLink interface utilities for communicating with vehicles via mavlink2rest
"""

import os
import aiohttp
from typing import Optional

# Module-level variables for MAVLink and vehicle IDs
_mavlink_system_id: Optional[int] = None
_mavlink_component_id: Optional[int] = None

# MAVLink sequence tracking
_mavlink_sequence: int = 0

# COMMAND_LONG message template for SET_MESSAGE_INTERVAL
COMMAND_LONG_SET_MESSAGE_INTERVAL_TEMPLATE = """{{
  "header": {{
    "system_id": {sysid},
    "component_id": {component_id},
    "sequence": {sequence}
  }},
  "message": {{
    "type": "COMMAND_LONG",
    "target_system": {target_system},
    "target_component": {target_component},
    "command": {{
      "type": "MAV_CMD_SET_MESSAGE_INTERVAL"
    }},
    "confirmation": 0,
    "param1": {message_id},
    "param2": {interval_us},
    "param3": {param3},
    "param4": {param4},
    "param5": {param5},
    "param6": {param6},
    "param7": {response_target}
  }}
}}"""


def init_sysids() -> None:
    """Initialize MAVLink system and component IDs from environment variables"""
    global _mavlink_system_id, _mavlink_component_id

    if _mavlink_system_id is None:
        _mavlink_system_id = int(os.environ.get("MAV_SYSTEM_ID", 1))
        print(f"MAVLink SysId: {_mavlink_system_id}")
    if _mavlink_component_id is None:
        _mavlink_component_id = int(os.environ.get("MAV_COMPONENT_ID_ONBOARD_COMPUTER", 191))
        print(f"MAVLink CompId: {_mavlink_component_id}")


async def get_vehicle_location(mavlink2rest_url: str) -> Optional[tuple]:
    """Get current GPS location from vehicle via mavlink2rest
       using GLOBAL_POSITION_INT (preferred) or GPS_RAW_INT (fallback) messages

    Args:
        mavlink2rest_url: URL of the mavlink2rest service

    Returns:
        tuple: (lat, lon, alt) in decimal degrees and meters, or None if unavailable
    """
    global _mavlink_system_id

    if not mavlink2rest_url:
        return None

    # Initialize IDs if needed
    init_sysids()

    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Try GLOBAL_POSITION_INT first (fused EKF estimate), then fall back to GPS_RAW_INT (raw GPS)
            message_types = ['GLOBAL_POSITION_INT', 'GPS_RAW_INT']

            for msg_type in message_types:
                api_path = f"{mavlink2rest_url}/mavlink/vehicles/{_mavlink_system_id}/components/1/messages/{msg_type}/message"

                try:
                    async with session.get(api_path) as response:
                        if response.status == 200:
                            data = await response.json()

                            # Extract GPS data (direct format from /message endpoint)
                            lat_raw = data.get('lat', 0)
                            lon_raw = data.get('lon', 0)
                            alt_raw = data.get('alt', 0)  # Both message types use mm above MSL

                            # Check if we have valid location data
                            if lat_raw != 0 and lon_raw != 0:
                                # Convert from MAVLink units (1e7 for lat/lon, mm for alt)
                                lat = lat_raw * 1e-7
                                lon = lon_raw * 1e-7
                                alt = alt_raw * 1e-3  # mm to meters

                                return (lat, lon, alt)
                except Exception:
                    continue

    except Exception:
        pass

    return None


async def send_rtcm_to_mavlink(
    mavlink2rest_url: str,
    rtcm_data: bytes,
    fragment_id: int,
    is_fragmented: bool,
    rtcm_sequence: int
) -> bool:
    """Send RTCM data via mavlink2rest GPS_RTCM_DATA message

    Args:
        mavlink2rest_url: URL of the mavlink2rest service
        rtcm_data: RTCM data bytes to send
        fragment_id: Fragment ID (0-3)
        is_fragmented: Whether this is a fragmented message
        rtcm_sequence: RTCM sequence number (0-31)

    Returns:
        bool: True if sent successfully, False otherwise
    """
    global _mavlink_system_id, _mavlink_component_id, _mavlink_sequence

    if not mavlink2rest_url:
        print("⚠️  Warning: mavlink2rest URL not configured, skipping RTCM data")
        return False

    # Initialize IDs if needed
    init_sysids()

    # Increment sequence number (wrap at 255 for MAVLink)
    _mavlink_sequence = (_mavlink_sequence + 1) % 256

    # Calculate flags according to GPS_RTCM_DATA specification:
    # Bit 0 (LSB): 1 = fragmented, 0 = not fragmented
    # Bits 1-2: Fragment ID (0-3)
    # Bits 3-7: RTCM Sequence ID (0-31)
    flags = 0
    if is_fragmented:
        flags |= 0x01  # Set fragmented bit
    flags |= (fragment_id & 0x03) << 1  # Fragment ID (2 bits, ensure it's 0-3)
    flags |= (rtcm_sequence & 0x1F) << 3  # RTCM sequence (5 bits, ensure it's 0-31)

    # Validation
    if fragment_id > 3:
        print(f"⚠️  Warning: Fragment ID {fragment_id} > 3, this should not happen!")
    if rtcm_sequence > 31:
        print(f"⚠️  Warning: RTCM sequence {rtcm_sequence} > 31, this should not happen!")

    # GPS_RTCM_DATA message format
    message = {
        "header": {
            "system_id": _mavlink_system_id,
            "component_id": _mavlink_component_id,
            "sequence": _mavlink_sequence
        },
        "message": {
            "type": "GPS_RTCM_DATA",
            "flags": flags,
            "len": len(rtcm_data),
            "data": list(rtcm_data)  # Convert to list of ints
        }
    }

    # Send to mavlink2rest
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{mavlink2rest_url}/mavlink",
                json=message,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status != 200:
                    response_text = await response.text()
                    raise Exception(f"mavlink2rest error {response.status}: {response_text}")
                return True

    except Exception as e:
        print(f"⚠️  Warning: Failed to send to mavlink2rest: {e}")
        return False


async def send_set_message_interval(
    mavlink2rest_url: str,
    message_id: int,
    interval_hz: float = 1.0
) -> bool:
    """Send a SET_MESSAGE_INTERVAL command to request a MAVLink message from the vehicle at a specified rate

    Args:
        mavlink2rest_url: URL of the mavlink2rest service
        message_id: MAVLink message ID to request (e.g. 33 for GLOBAL_POSITION_INT)
        interval_hz: Frequency in Hz (default 1.0 for 1Hz)

    Returns:
        bool: True if request sent successfully, False otherwise
    """
    global _mavlink_system_id

    if not mavlink2rest_url:
        print("⚠️  No mavlink2rest URL configured")
        return False

    # Initialize IDs if needed
    init_sysids()

    # Increment sequence number (wrap at 255 for MAVLink)
    _mavlink_sequence = (_mavlink_sequence + 1) % 256

    interval_us = int(1000000 / interval_hz)

    message_json = COMMAND_LONG_SET_MESSAGE_INTERVAL_TEMPLATE.format(
        sysid=_mavlink_system_id,
        component_id=_mavlink_component_id,
        sequence=_mavlink_sequence,
        target_system=_mavlink_system_id,
        target_component=1,
        message_id=message_id,
        interval_us=interval_us,
        param3=0,
        param4=0,
        param5=0,
        param6=0,
        response_target=0
    )

    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{mavlink2rest_url}/mavlink",
                data=message_json,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status == 200:
                    print(f"✅ Requested message ID {message_id} at {interval_hz:.1f}Hz from vehicle (sys:{_mavlink_system_id}, comp:1)")
                    return True
                else:
                    response_text = await response.text()
                    print(f"⚠️  Failed to request message ID {message_id}: HTTP {response.status}: {response_text}")
                    return False
    except Exception as e:
        print(f"⚠️  Failed to send message interval request: {e}")
        return False
