#!/usr/bin/env python3

import asyncio
import requests
import logging.handlers
import json
import base64
import aiohttp
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from litestar import Litestar, get, post, MediaType
from litestar.controller import Controller
from litestar.datastructures import State
from litestar.logging import LoggingConfig
from litestar.static_files.config import StaticFilesConfig
from pydantic import BaseModel

# Global configuration that will be set by command line args
_global_config = {
    'mavlink2rest_url': 'http://host.docker.internal:6040',
    'config_file': 'config/rtk_config.json'
}

class RTKConfig(BaseModel):
    ntrip_url: str = ""
    username: str = ""
    password: str = ""
    mountpoint: str = ""
    mavlink2rest_url: str = "http://host.docker.internal:6040"
    enabled: bool = False

class RTKStatus(BaseModel):
    connected: bool = False
    last_update: Optional[str] = None
    bytes_received: int = 0
    error_message: Optional[str] = None
    vehicle_location: Optional[Dict[str, float]] = None  # {"lat": x, "lon": y, "alt": z}
    last_gga_sent_time: Optional[str] = None  # When we last sent GGA to NTRIP server
    gga_send_success: bool = False  # Whether last GGA send was successful

class ConfigManager:
    """Manages configuration storage in a local JSON file"""

    def __init__(self, config_file: str = "config/rtk_config.json"):
        self.config_file = Path(config_file)
        self.config_file.parent.mkdir(parents=True, exist_ok=True)

    def load_config(self) -> RTKConfig:
        """Load configuration from file"""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    data = json.load(f)
                # Use global mavlink2rest_url if not in saved config
                if 'mavlink2rest_url' not in data:
                    data['mavlink2rest_url'] = _global_config['mavlink2rest_url']
                return RTKConfig(**data)
        except Exception as e:
            print(f"Warning: Could not load config from {self.config_file}: {e}")

        # Return default config with global mavlink2rest_url
        return RTKConfig(mavlink2rest_url=_global_config['mavlink2rest_url'])

    def save_config(self, config: RTKConfig) -> bool:
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config.model_dump(), f, indent=2)
            return True
        except Exception as e:
            print(f"Warning: Could not save config to {self.config_file}: {e}")
            return False

class RTCMParser:
    """Parse RTCM 3.x messages from streaming data"""

    def __init__(self):
        self.buffer = bytearray()
        self.messages_parsed = 0

    def add_data(self, data: bytes) -> list:
        """Add new data and return list of complete RTCM messages"""
        self.buffer.extend(data)
        messages = []

        while len(self.buffer) >= 6:  # Minimum RTCM message size (header + CRC)
            # Look for RTCM preamble (0xD3)
            preamble_idx = self.buffer.find(0xD3)
            if preamble_idx == -1:
                # No preamble found, clear buffer except last few bytes
                if len(self.buffer) > 100:
                    self.buffer = self.buffer[-10:]
                break

            # Remove data before preamble
            if preamble_idx > 0:
                self.buffer = self.buffer[preamble_idx:]

            # Check if we have enough data for header
            if len(self.buffer) < 3:
                break

            # Parse RTCM header: preamble(8) + reserved(6) + length(10) = 24 bits = 3 bytes
            header = int.from_bytes(self.buffer[0:3], 'big')
            preamble = (header >> 16) & 0xFF
            reserved = (header >> 10) & 0x3F
            length = header & 0x3FF

            # Verify preamble and reserved field
            if preamble != 0xD3 or reserved != 0:
                # Invalid header, skip this byte and continue searching
                self.buffer = self.buffer[1:]
                continue

            # Calculate total message size (header + payload + CRC)
            total_size = 3 + length + 3  # 3 header + payload + 3 CRC

            # Check if we have the complete message
            if len(self.buffer) < total_size:
                break  # Wait for more data

            # Extract complete message
            message = bytes(self.buffer[0:total_size])

            # Verify CRC (simplified check - just verify it's present)
            if self.is_valid_rtcm_message(message):
                messages.append(message)
                self.messages_parsed += 1

            # Remove processed message from buffer
            self.buffer = self.buffer[total_size:]

        return messages

    def is_valid_rtcm_message(self, message: bytes) -> bool:
        """Basic validation of RTCM message (simplified)"""
        if len(message) < 6:
            return False

        # Check preamble
        if message[0] != 0xD3:
            return False

        # Check length field consistency
        header = int.from_bytes(message[0:3], 'big')
        length = header & 0x3FF
        expected_total = 3 + length + 3

        return len(message) == expected_total

def generate_gga_message(lat: float, lon: float, alt: float) -> bytes:
    """Generate a GGA NMEA message for NTRIP location reporting

    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees  
        alt: Altitude in meters

    Returns:
        Complete GGA message as bytes including CRLF
    """

    # Convert decimal degrees to degrees and minutes
    lat_deg = int(abs(lat))
    lat_min = (abs(lat) - lat_deg) * 60.0
    lat_ns = 'N' if lat >= 0 else 'S'

    lon_deg = int(abs(lon))
    lon_min = (abs(lon) - lon_deg) * 60.0
    lon_ew = 'E' if lon >= 0 else 'W'

    # Get current time
    now = datetime.utcnow()
    time_str = now.strftime('%H%M%S.%f')[:-3]  # HHMMSS.sss

    # Build GGA message (without checksum)
    gga_data = (f"GPGGA,{time_str},"
                f"{lat_deg:02d}{lat_min:07.4f},{lat_ns},"
                f"{lon_deg:03d}{lon_min:07.4f},{lon_ew},"
                f"1,08,1.0,{alt:.1f},M,0.0,M,,")

    # Calculate NMEA checksum
    checksum = 0
    for char in gga_data:
        checksum ^= ord(char)

    # Complete message with $ prefix and checksum
    complete_message = f"${gga_data}*{checksum:02X}\r\n"
    return complete_message.encode('ascii')

class RTKController(Controller):
    def __init__(self, owner: "Litestar") -> None:
        super().__init__(owner)
        self._config_manager = ConfigManager(_global_config['config_file'])
        self._config = self._config_manager.load_config()
        self._status = RTKStatus()
        self._ntrip_task = None
        self._mavlink_sequence = 0  # Track MAVLink message sequence
        self._rtcm_sequence = 0     # Track RTCM sequence (5 bits, 0-31)
        self._rtcm_parser = RTCMParser()  # Parse RTCM message boundaries
        self._last_gga_time = 0     # Track when we last sent a GGA message
        self._last_gga_success = False  # Track if last GGA send was successful
        self._vehicle_system_id = None  # Discovered vehicle system ID
        self._vehicle_location = None  # vehicle GPS location (lat, lon, alt) in decimal degrees and meters
        self._vehicle_location_update = 0  # system time (in sec since epoch) when vehicle location received

    def _is_config_valid(self) -> bool:
        """Check if the current configuration is valid for connecting"""
        if not self._config.ntrip_url.strip():
            return False
        if not self._config.mountpoint.strip():
            return False
        if not self._config.mavlink2rest_url.strip():
            return False
        # Username and password are optional (some services don't require them)
        return True

    async def auto_start_if_enabled(self) -> None:
        """Auto-start NTRIP connection if settings are valid and enabled"""
        if self._is_config_valid() and self._config.enabled:
            print("🚀 Auto-starting NTRIP connection with saved settings...")
            await self._start_ntrip_connection()
        elif self._config.enabled:
            print("⚠️  NTRIP connection enabled but settings are invalid - please check configuration")
            self._status.error_message = "Invalid configuration - please check NTRIP settings"
        else:
            print("ℹ️  NTRIP connection disabled - use web interface to configure and enable")

    @get("/config", sync_to_thread=False)
    def get_config(self) -> Dict[str, Any]:
        """Get current RTK configuration"""
        return self._config.model_dump()

    @post("/config", sync_to_thread=False)
    def set_config(self, data: RTKConfig) -> Dict[str, Any]:
        """Set RTK configuration"""
        self._config = data

        # Save to local file
        if self._config_manager.save_config(data):
            print(f"Configuration saved to {self._config_manager.config_file}")

        # Start/stop NTRIP connection based on enabled status and validity
        if data.enabled:
            if self._is_config_valid():
                print("🚀 Starting NTRIP connection...")
                asyncio.create_task(self._start_ntrip_connection())
            else:
                # Provide specific error message about what's missing
                missing_fields = []
                if not data.ntrip_url.strip():
                    missing_fields.append("NTRIP Caster URL")
                if not data.mountpoint.strip():
                    missing_fields.append("Mountpoint")
                if not data.mavlink2rest_url.strip():
                    missing_fields.append("mavlink2rest URL")

                error_msg = f"Invalid configuration: Missing {', '.join(missing_fields)}"
                self._status.connected = False
                self._status.error_message = error_msg
                print(f"⚠️  {error_msg}")
                return {"status": "error", "message": error_msg, "config": data.model_dump()}
        else:
            if self._ntrip_task:
                self._ntrip_task.cancel()
                self._ntrip_task = None
            self._status.connected = False
            self._status.error_message = "Connection disabled"
            print("⏹️  NTRIP connection disabled")

        return {"status": "success", "config": data.model_dump()}

    @get("/status", sync_to_thread=False)
    def get_status(self) -> Dict[str, Any]:
        """Get current RTK status"""
        status = self._status.model_dump()

        # Add current GPS location info if available
        if self._vehicle_location:
            lat, lon, alt = self._vehicle_location
            status['vehicle_location'] = {
                "lat": lat,
                "lon": lon, 
                "alt": alt
            }
            status['vehicle_location_time'] = datetime.utcfromtimestamp(self._vehicle_location_update).isoformat() + 'Z'
        
        # Add GGA send status
        if self._last_gga_time > 0:
            status['last_gga_sent_time'] = datetime.utcfromtimestamp(self._last_gga_time).isoformat() + 'Z'
        status['gga_send_success'] = self._last_gga_success

        return status

    async def _start_ntrip_connection(self):
        """Start NTRIP client connection and forward RTCM data to mavlink2rest"""
        if self._ntrip_task:
            self._ntrip_task.cancel()

        self._ntrip_task = asyncio.create_task(self._ntrip_client_loop())

    async def _ntrip_client_loop(self):
        """Main NTRIP client loop - retries on ALL errors and disconnections"""
        reconnect_delay = 5  # Start with 5 seconds
        max_reconnect_delay = 300  # Max 5 minutes
        consecutive_failures = 0

        print("🔄 Starting NTRIP client loop with automatic reconnection")

        # Main reconnection loop - this will ALWAYS retry unless explicitly cancelled
        while True:
            connection_start_time = datetime.now()

            try:
                # Validate configuration before attempting connection
                if not self._config.ntrip_url:
                    raise Exception("NTRIP URL not configured")
                if not self._config.mountpoint:
                    raise Exception("NTRIP mountpoint not configured")

                print(f"🔗 Connection attempt #{consecutive_failures + 1}")

                # Attempt connection and data streaming
                await self._connect_ntrip()

                # If we reach here, the connection ended (which shouldn't happen in normal operation)
                # This means the connection was lost and we should retry
                raise Exception("Connection ended unexpectedly")

            except asyncio.CancelledError:
                # This is the ONLY exception that should stop the retry loop
                self._status.connected = False
                self._status.error_message = "Connection cancelled by user"
                self._status.last_update = datetime.now().isoformat()
                print("🛑 NTRIP client loop cancelled by user request")
                break  # Exit the retry loop

            except Exception as e:
                # ALL other exceptions trigger a retry attempt
                consecutive_failures += 1
                connection_duration = (datetime.now() - connection_start_time).total_seconds()

                # Update status to reflect the error
                self._status.connected = False
                self._status.error_message = str(e)
                self._status.last_update = datetime.now().isoformat()

                # Calculate exponential backoff delay
                reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

                # Log the error and retry information
                print(f"❌ NTRIP connection failed (attempt #{consecutive_failures})")
                print(f"   Error: {e}")
                print(f"   Connection duration: {connection_duration:.1f} seconds")
                print(f"   Error type: {type(e).__name__}")

                # If connection was very short, it might be a configuration issue
                if connection_duration < 5.0 and consecutive_failures >= 3:
                    print(f"   ⚠️  Rapid failures detected - possible configuration issue")

                print(f"⏳ Retrying in {reconnect_delay:.1f} seconds...")

                try:
                    await asyncio.sleep(reconnect_delay)
                except asyncio.CancelledError:
                    # Even during sleep, respect cancellation
                    self._status.connected = False
                    self._status.error_message = "Connection cancelled during retry delay"
                    self._status.last_update = datetime.now().isoformat()
                    print("🛑 NTRIP client loop cancelled during retry delay")
                    break

            # Reset failure counter only after a successful connection that lasted some time
            # (If _connect_ntrip returns normally, we'll reset it there)

        print("🏁 NTRIP client loop terminated")

    async def _connect_ntrip(self):
        """Connect to NTRIP caster and forward RTCM data - raises exception on ANY error"""
        connection_established = False
        reader = None
        writer = None

        try:
            # Parse URL properly
            url_parts = self._config.ntrip_url.replace('http://', '').replace('https://', '')
            if ':' in url_parts:
                host, port_str = url_parts.split(':', 1)
                port = int(port_str)
            else:
                host = url_parts
                port = 2101  # Default NTRIP port

            print(f"🔗 Connecting to NTRIP caster: {host}:{port}")
            print(f"📍 Mountpoint: {self._config.mountpoint}")

            # Debug credentials (mask password for security)
            if self._config.username or self._config.password:
                masked_password = '*' * len(self._config.password) if self._config.password else '(empty)'
                print(f"🔐 Using credentials: {self._config.username} / {masked_password}")
            else:
                print("🔓 No credentials provided (anonymous access)")

            # Connect to NTRIP caster with timeout
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=30.0  # 30 second connection timeout
                )
                print(f"✅ TCP connection established to {host}:{port}")
            except asyncio.TimeoutError:
                raise Exception(f"Connection timeout to {host}:{port} (30 seconds)")
            except Exception as e:
                raise Exception(f"Failed to connect to {host}:{port}: {e}")

            # Send NTRIP request
            auth = f"{self._config.username}:{self._config.password}"
            auth_encoded = base64.b64encode(auth.encode()).decode()

            # Debug the exact credentials being encoded
            print(f"🔍 Debug credential encoding:")
            print(f"   Raw auth string length: {len(auth)} characters")
            print(f"   Username length: {len(self._config.username)} characters")
            print(f"   Password length: {len(self._config.password)} characters")

            # Check for potentially problematic characters
            special_chars = ['*', '@', '#', '$', '%', '^', '&', '!', '?', '<', '>', '{', '}', '[', ']', '\\', '/', '|', '~', '`']
            found_special = [char for char in special_chars if char in self._config.password]
            if found_special:
                print(f"   ⚠️  Special characters found in password: {found_special}")

            # Show a few characters of the base64 for verification (but not the whole thing for security)
            auth_preview = auth_encoded[:12] + "..." + auth_encoded[-4:] if len(auth_encoded) > 16 else auth_encoded
            print(f"   Base64 encoded (preview): {auth_preview}")

            # Check for common encoding issues
            try:
                # Verify the base64 can be decoded back
                decoded_check = base64.b64decode(auth_encoded).decode()
                if decoded_check == auth:
                    print(f"   ✅ Base64 encoding/decoding verified")
                else:
                    print(f"   ❌ Base64 encoding verification failed!")
            except Exception as e:
                print(f"   ❌ Base64 encoding error: {e}")

            # Debug the exact request being sent
            request = (f"GET /{self._config.mountpoint} HTTP/1.0\r\n"
                      f"Host: {host}:{port}\r\n"
                      f"User-Agent: NTRIP BlueOS-RTK-Extension/1.0\r\n"
                      f"Authorization: Basic {auth_encoded}\r\n"
                      f"\r\n")

            print(f"📤 Sending NTRIP request:")
            print(f"   GET /{self._config.mountpoint} HTTP/1.0")
            print(f"   Host: {host}:{port}")
            print(f"   User-Agent: NTRIP BlueOS-RTK-Extension/1.0")
            print(f"   Authorization: Basic {auth_encoded[:8]}...")

            # Debug the exact bytes being sent
            request_bytes = request.encode()
            print(f"📤 Raw request ({len(request_bytes)} bytes):")
            print(f"   {repr(request_bytes)}")

            writer.write(request_bytes)
            await writer.drain()
            print(f"✅ Request sent successfully")

            # Read response header with timeout
            print(f"📥 Reading server response...")
            try:
                response = await asyncio.wait_for(reader.readline(), timeout=30.0)
                response_str = response.decode().strip()
                print(f"📥 Server response: {response_str}")
            except asyncio.TimeoutError:
                raise Exception("Timeout waiting for server response (30 seconds)")

            # Check response - handle both NTRIP v1 (ICY 200 OK) and v2 (HTTP/1.x 200 OK)
            response_is_ok = (b"200 OK" in response or b"ICY 200 OK" in response)
            
            if not response_is_ok:
                # Read more response lines for better debugging
                additional_lines = []
                try:
                    for _ in range(5):  # Read up to 5 more lines
                        line = await asyncio.wait_for(reader.readline(), timeout=1.0)
                        if line:
                            additional_lines.append(line.decode().strip())
                        else:
                            break
                except asyncio.TimeoutError:
                    pass

                error_details = response_str
                if additional_lines:
                    error_details += " | Additional info: " + " | ".join(additional_lines)

                # Provide specific error messages based on response
                if "401" in response_str or "Unauthorized" in response_str:
                    raise Exception(f"Authentication failed - check username/password. Server response: {error_details}")
                elif "404" in response_str or "Not Found" in response_str:
                    raise Exception(f"Mountpoint '{self._config.mountpoint}' not found. Server response: {error_details}")
                elif "403" in response_str or "Forbidden" in response_str:
                    raise Exception(f"Access forbidden - check permissions. Server response: {error_details}")
                else:
                    raise Exception(f"NTRIP server error: {error_details}")

            print(f"✅ Authentication successful!")

            # Skip remaining headers
            print(f"📥 Reading response headers...")
            header_count = 0
            while True:
                line = await reader.readline()
                if line == b'\r\n' or line == b'\n':
                    break
                header_count += 1
                if header_count <= 3:  # Show first few headers for debugging
                    print(f"   Header: {line.decode().strip()}")

            print(f"📥 Read {header_count} response headers")

            # Mark connection as successfully established
            connection_established = True
            self._status.connected = True
            self._status.error_message = None
            self._status.last_update = datetime.now().isoformat()
            print(f"🎉 Connected to NTRIP caster, starting to receive RTCM data...")

            # Reset failure counter after successful connection establishment
            if hasattr(self, '_ntrip_client_loop'):
                # Reset will happen in the main loop after we return successfully
                pass

            # Reset RTCM parser for new connection
            self._rtcm_parser = RTCMParser()

            # Send GPS location if available
            print(f"📍 Sending initial GGA location message...")
            try:
                await self._send_gga_message(writer)
                self._last_gga_success = True
                print(f"✅ Initial GGA message sent successfully")
            except Exception as e:
                self._last_gga_success = False
                print(f"⚠️  Warning: Failed to send initial GGA message: {e}")

            # Forward RTCM data to mavlink2rest
            data_chunks = 0
            total_rtcm_bytes = 0
            total_rtcm_messages = 0
            total_mavlink_messages = 0
            failed_mavlink_sends = 0
            last_data_time = datetime.now()
            read_timeout = 60.0  # 60 second timeout for data reads

            # Main data reading loop - ANY error here will trigger reconnection
            while True:
                try:
                    # Read data with timeout to detect stalled connections
                    data = await asyncio.wait_for(reader.read(1024), timeout=read_timeout)

                    if not data:
                        # Connection closed by server - this is a disconnect that should trigger reconnection
                        print(f"🔌 NTRIP server closed connection")
                        print(f"   Session stats:")
                        print(f"   • Total chunks received: {data_chunks}")
                        print(f"   • Total RTCM bytes: {total_rtcm_bytes}")
                        print(f"   • Total RTCM messages parsed: {total_rtcm_messages}")
                        print(f"   • Total MAVLink messages sent: {total_mavlink_messages}")
                        print(f"   • Failed MAVLink sends: {failed_mavlink_sends}")

                        # This will trigger reconnection
                        raise Exception("NTRIP server closed connection")

                    # Update data reception tracking
                    data_chunks += 1
                    chunk_size = len(data)
                    total_rtcm_bytes += chunk_size
                    self._status.bytes_received += chunk_size
                    self._status.last_update = datetime.now().isoformat()
                    last_data_time = datetime.now()

                    # Send periodic GGA messages - many NTRIP servers require this to maintain connection
                    current_time = time.time()
                    if current_time - self._last_gga_time > 10.0:  # Send every 10 seconds
                        try:
                            await self._send_gga_message(writer)
                            self._last_gga_success = True
                            print(f"📍 Periodic GGA message sent")
                        except Exception as e:
                            self._last_gga_success = False
                            print(f"⚠️  Warning: Failed to send periodic GGA message: {e}")
                            # Don't fail the connection for this

                    # Log periodic progress with more detail
                    if data_chunks % 50 == 0:
                        print(f"📡 Progress: {data_chunks} chunks, {total_rtcm_bytes} RTCM bytes, {total_rtcm_messages} RTCM msgs, {total_mavlink_messages} MAVLink msgs")

                    # Parse RTCM messages from this chunk
                    try:
                        rtcm_messages = self._rtcm_parser.add_data(data)
                        total_rtcm_messages += len(rtcm_messages)

                        # Process each complete RTCM message
                        for rtcm_message in rtcm_messages:
                            # Increment RTCM sequence for each complete RTCM message (wrap at 31 for 5-bit field)
                            self._rtcm_sequence = (self._rtcm_sequence + 1) % 32

                            # Split RTCM message into 180-byte chunks for MAVLink
                            mavlink_chunks_this_msg = 0
                            chunks = []
                            for i in range(0, len(rtcm_message), 180):
                                chunks.append(rtcm_message[i:i+180])

                            # Send each chunk with proper fragmentation flags
                            for fragment_index, chunk in enumerate(chunks):
                                # Determine if this message is fragmented
                                is_fragmented = len(chunks) > 1
                                # Fragment ID is only 2 bits (0-3), so wrap at 4
                                fragment_id = fragment_index % 4
                                is_last_fragment = fragment_index == len(chunks) - 1

                                # Try to send this chunk
                                send_success = await self._send_rtcm_to_mavlink(
                                    chunk,
                                    fragment_id,
                                    is_fragmented,
                                    is_last_fragment,
                                    self._rtcm_sequence
                                )
                                if send_success:
                                    total_mavlink_messages += 1
                                    mavlink_chunks_this_msg += 1
                                else:
                                    failed_mavlink_sends += 1

                            # Log detailed per-RTCM message info occasionally
                            if total_rtcm_messages % 50 == 0:
                                chunks_expected = len(chunks)
                                rtcm_msg_size = len(rtcm_message)
                                print(f"📊 RTCM msg #{total_rtcm_messages}: {rtcm_msg_size} bytes → {chunks_expected} MAVLink msgs ({mavlink_chunks_this_msg} sent)")
                                print(f"   RTCM seq: {self._rtcm_sequence}, Fragmented: {len(chunks) > 1}")
                                if len(chunks) > 4:
                                    print(f"   ⚠️  Large fragmentation: {len(chunks)} fragments will wrap fragment IDs")

                    except Exception as e:
                        print(f"⚠️  Error processing RTCM data in chunk {data_chunks}: {e}")
                        # Continue processing - don't let RTCM parsing errors kill the connection

                except asyncio.TimeoutError:
                    # No data received within timeout period - check if connection is still alive
                    time_since_data = (datetime.now() - last_data_time).total_seconds()
                    print(f"⏰ No data received for {time_since_data:.1f} seconds")

                    if time_since_data > 120:  # More than 2 minutes without data
                        print(f"🔌 Connection appears stalled - triggering reconnection")
                        raise Exception("Connection stalled - no data received for over 2 minutes")
                    else:
                        # Continue waiting for data
                        print(f"   Continuing to wait for data...")
                        continue

                except Exception as e:
                    # ANY other error during data reading should trigger reconnection
                    print(f"❌ Error reading from NTRIP stream: {e}")
                    raise Exception(f"Data read error: {str(e)}")

        except asyncio.CancelledError:
            # Pass cancellation up to the main loop
            raise

        except Exception as e:
            # Mark connection as failed and ensure status is updated
            if connection_established:
                print(f"❌ Connection lost after successful establishment: {e}")
            else:
                print(f"❌ Failed to establish connection: {e}")

            # Make sure status reflects the error
            self._status.connected = False
            self._status.error_message = str(e)
            self._status.last_update = datetime.now().isoformat()

            # Re-raise to trigger retry in main loop
            raise

        finally:
            # Always clean up the connection, regardless of how we got here
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                    print(f"🔌 NTRIP connection closed cleanly")
                except Exception as e:
                    print(f"⚠️  Error closing connection: {e}")

            # Update status if connection was lost
            if connection_established:
                self._status.connected = False
                if not self._status.error_message or self._status.error_message == "None":
                    self._status.error_message = "Connection lost - reconnecting"
                self._status.last_update = datetime.now().isoformat()

    async def _discover_vehicle_system_id(self) -> Optional[int]:
        """Discover vehicle system ID by finding first vehicle with GPS data

        Returns:
            int: System ID of vehicle with GPS, or None if not found
        """
        if not self._config.mavlink2rest_url:
            return None

        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Get list of vehicles from mavlink2rest
                vehicles_path = f"{self._config.mavlink2rest_url}/mavlink/vehicles"
                async with session.get(vehicles_path) as response:
                    if response.status == 200:
                        vehicles_data = await response.json()

                        # Extract vehicle IDs (keys are system IDs)
                        if isinstance(vehicles_data, dict):
                            # Keys might be strings or integers - convert to int and filter out non-numeric
                            vehicle_ids = sorted([int(vid) for vid in vehicles_data.keys() if str(vid).isdigit()])
                            if vehicle_ids:
                                print(f"✅ Found vehicles: {vehicle_ids}")

                                # Try each vehicle to see if it has location data
                                message_types = ['GLOBAL_POSITION_INT', 'GPS_RAW_INT']
                                for sys_id in vehicle_ids:
                                    for msg_type in message_types:
                                        api_path = f"{self._config.mavlink2rest_url}/mavlink/vehicles/{sys_id}/components/1/messages/{msg_type}/message"
                                        try:
                                            async with session.get(api_path) as msg_response:
                                                if msg_response.status == 200:
                                                    data = await msg_response.json()
                                                    lat_raw = data.get('lat', 0)
                                                    lon_raw = data.get('lon', 0)
                                                    if lat_raw != 0 and lon_raw != 0:
                                                        print(f"✅ Using vehicle SysID {sys_id}, {msg_type}: lat={lat_raw}, lon={lon_raw}")
                                                        return sys_id
                                        except Exception as e:
                                            print(f"      {msg_type}: Error - {e}")
                                            continue

                                # If no vehicle has GPS data yet, use the first/lowest ID
                                print(f"⚠️  No vehicle has GPS data yet, using lowest ID: {vehicle_ids[0]}")
                                return vehicle_ids[0]
        except Exception as e:
            print(f"⚠️  Could not discover vehicle system ID: {e}")

        # Fall back to system ID 1 if discovery fails
        print(f"⚠️  Using default system ID: 1")
        return 1

    async def _get_vehicle_location(self) -> Optional[tuple]:
        """Get current GPS location from vehicle via mavlink2rest
           using GLOBAL_POSITION_INT (preferred) or GPS_RAW_INT (fallback) messages

        Returns:
            tuple: (lat, lon, alt) in decimal degrees and meters, or None if unavailable
        """
        if not self._config.mavlink2rest_url:
            print("⚠️  No mavlink2rest URL configured")
            return None

        # Track if this is first attempt for debug logging
        if not hasattr(self, '_gps_fetch_logged'):
            self._gps_fetch_logged = False

        # Discover vehicle system ID on first call
        if self._vehicle_system_id is None:
            self._vehicle_system_id = await self._discover_vehicle_system_id()

        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Try GLOBAL_POSITION_INT first (fused EKF estimate), then fall back to GPS_RAW_INT (raw GPS)
                message_types = ['GLOBAL_POSITION_INT', 'GPS_RAW_INT']

                for msg_type in message_types:
                    api_path = f"{self._config.mavlink2rest_url}/mavlink/vehicles/{self._vehicle_system_id}/components/1/messages/{msg_type}/message"

                    try:
                        async with session.get(api_path) as response:
                            if response.status == 200:
                                data = await response.json()

                                # Debug log on first successful fetch
                                if not self._gps_fetch_logged:
                                    print(f"🔍 GPS API: {api_path}")
                                    print(f"   Keys: {list(data.keys())}")

                                # Extract GPS data (direct format from /message endpoint)
                                lat_raw = data.get('lat', 0)
                                lon_raw = data.get('lon', 0)
                                alt_raw = data.get('alt', 0)  # Both message types use mm above MSL

                                # Debug on first fetch
                                if not self._gps_fetch_logged:
                                    print(f"   {msg_type} data: lat={lat_raw}, lon={lon_raw}, alt={alt_raw}")

                                # Check if we have valid location data
                                if lat_raw != 0 and lon_raw != 0:
                                    # Convert from MAVLink units (1e7 for lat/lon, mm for alt)
                                    lat = lat_raw * 1e-7
                                    lon = lon_raw * 1e-7
                                    alt = alt_raw * 1e-3  # mm to meters

                                    self._vehicle_location = (lat, lon, alt)
                                    self._vehicle_location_update = time.time()

                                    # Log first successful location
                                    if not self._gps_fetch_logged:
                                        print(f"✅ GPS location from {msg_type} (system {self._vehicle_system_id}): {lat:.6f}°, {lon:.6f}°, {alt:.1f}m")
                                        self._gps_fetch_logged = True

                                    return self._vehicle_location
                    except Exception as e:
                        if not self._gps_fetch_logged:
                            print(f"   {msg_type} fetch failed: {e}")
                        continue

                # If we get here, neither message type worked - use cached location if available
                if self._vehicle_location:
                    return self._vehicle_location

        except Exception as e:
            if not self._gps_fetch_logged:
                print(f"❌ GPS fetch error: {e}")
                self._gps_fetch_logged = True
            
            # On error, use cached location if available
            if self._vehicle_location:
                return self._vehicle_location

        return None

    async def _send_gga_message(self, writer) -> None:
        """Send GGA message to NTRIP server to report location
        
        Many NTRIP servers require periodic location updates to maintain the connection.
        This sends a GGA NMEA message with the current vehicle location from GPS.
        """
        try:
            # Get current vehicle location
            location = await self._get_vehicle_location()
            
            if location is None:
                raise Exception("No GPS location available from vehicle")
            
            lat, lon, alt = location
            
            gga_message = generate_gga_message(lat, lon, alt)
            writer.write(gga_message)
            await writer.drain()
            self._last_gga_time = time.time()
            
        except Exception as e:
            raise Exception(f"Failed to send GGA message: {e}")

    async def _send_rtcm_to_mavlink(self, rtcm_data: bytes, fragment_id: int, is_fragmented: bool, is_last_fragment: bool, rtcm_sequence: int) -> bool:
        """Send RTCM data via mavlink2rest GPS_RTCM_DATA message

        Returns:
            bool: True if sent successfully, False otherwise
        """
        if not self._config.mavlink2rest_url:
            print("⚠️  Warning: mavlink2rest URL not configured, skipping RTCM data")
            return False

        # Increment sequence number (wrap at 255 for MAVLink)
        self._mavlink_sequence = (self._mavlink_sequence + 1) % 256

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
                "system_id": 255,
                "component_id": 220,
                "sequence": self._mavlink_sequence
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
                    f"{self._config.mavlink2rest_url}/mavlink",
                    json=message,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status != 200:
                        response_text = await response.text()
                        raise Exception(f"mavlink2rest error {response.status}: {response_text}")

                    # Log successful sends periodically with RTCM message context
                    if hasattr(self, '_mavlink_send_count'):
                        self._mavlink_send_count += 1
                    else:
                        self._mavlink_send_count = 1

                    # Reduced debugging frequency now that we know sequencing is correct
                    if self._mavlink_send_count % 200 == 0:
                        print(f"📡➡️  Sent {self._mavlink_send_count} MAVLink messages (seq: {self._mavlink_sequence}, rtcm_seq: {rtcm_sequence})")

                    return True

        except Exception as e:
            # Log first few errors in detail, then just count them
            if not hasattr(self, '_mavlink_error_count'):
                self._mavlink_error_count = 0

            self._mavlink_error_count += 1

            # Print more errors in detail to help with debugging
            if self._mavlink_error_count <= 10:
                print(f"⚠️  Warning: Failed to send to mavlink2rest (error #{self._mavlink_error_count}): {e}")
                print(f"   mavlink2rest URL: {self._config.mavlink2rest_url}")
                print(f"   RTCM data length: {len(rtcm_data)} bytes")
                print(f"   Error type: {type(e).__name__}")
                print(f"   Sequence: {self._mavlink_sequence}")

                # Print more details for specific error types
                if hasattr(e, 'status'):
                    print(f"   HTTP status: {e.status}")
                if hasattr(e, 'message'):
                    print(f"   Error message: {e.message}")

            elif self._mavlink_error_count % 25 == 0:
                print(f"⚠️  Warning: {self._mavlink_error_count} total mavlink2rest send failures")
                print(f"   Latest error: {e}")
                print(f"   mavlink2rest URL: {self._config.mavlink2rest_url}")

            return False

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='BlueOS RTK NTRIP Extension')
    parser.add_argument('--mavlink2rest-url',
                       default='http://host.docker.internal:6040',
                       help='mavlink2rest service URL (default: http://host.docker.internal:6040)')
    parser.add_argument('--host',
                       default='0.0.0.0',
                       help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--port',
                       type=int,
                       default=8000,
                       help='Port to bind to (default: 8000)')
    parser.add_argument('--config-file',
                       default='config/rtk_config.json',
                       help='Configuration file path (default: config/rtk_config.json)')
    parser.add_argument('--reload',
                       action='store_true',
                       help='Enable auto-reload for development')
    return parser.parse_args()

def setup_logging():
    """Setup logging configuration"""
    # Use relative path for logs
    log_dir = Path('./logs')
    log_dir.mkdir(parents=True, exist_ok=True)

    logging_config = LoggingConfig(
        loggers={
            __name__: dict(
                level='INFO',
                handlers=['queue_listener'],
            )
        },
    )

    fh = logging.handlers.RotatingFileHandler(log_dir / 'rtk.log', maxBytes=2**16, backupCount=1)
    return logging_config, fh

# Global reference to the controller for startup hook
_rtk_controller = None

async def startup_hook() -> None:
    """Application startup hook to auto-start NTRIP connection"""
    global _rtk_controller
    if _rtk_controller:
        await _rtk_controller.auto_start_if_enabled()

def create_app(args):
    """Create the Litestar application"""
    global _rtk_controller

    # Set global configuration
    _global_config['mavlink2rest_url'] = args.mavlink2rest_url
    _global_config['config_file'] = args.config_file

    logging_config, fh = setup_logging()

    # Determine static files directory based on current working directory
    static_dirs = []
    if Path('./static').exists():
        static_dirs = ['./static']
    elif Path('./app/static').exists():
        static_dirs = ['./app/static']
    else:
        print("Warning: Could not find static files directory")

    class RTKControllerSingleton(RTKController):
        """Singleton wrapper for RTKController to ensure single instance"""
        def __init__(self, owner: "Litestar") -> None:
            global _rtk_controller
            if _rtk_controller is None:
                super().__init__(owner)
                _rtk_controller = self
            # Return the existing instance
            self.__dict__ = _rtk_controller.__dict__

    app = Litestar(
        route_handlers=[RTKControllerSingleton],  # Use singleton wrapper
        state=State({}),  # Empty state since we're not using bag service
        static_files_config=[
            StaticFilesConfig(directories=static_dirs, path='/', html_mode=True)
        ] if static_dirs else [],
        logging_config=logging_config,
        on_startup=[startup_hook],  # Add startup hook
    )

    app.logger.addHandler(fh)
    return app

def main():
    """Main entry point"""
    args = parse_arguments()

    print("🛰️ BlueOS RTK Extension")
    print("=" * 50)
    print(f"mavlink2rest URL: {args.mavlink2rest_url}")
    print(f"Configuration file: {args.config_file}")
    print(f"Web interface: http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop")
    print()

    app = create_app(args)

    try:
        import uvicorn
        uvicorn.run(app, host=args.host, port=args.port, reload=args.reload)
    except KeyboardInterrupt:
        print("\n👋 Shutting down RTK Extension")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
