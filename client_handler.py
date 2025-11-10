#!/usr/bin/env python3
"""
Client handler for managing individual client connections.
Handles TCP stream buffering, command parsing, and message sending.
"""

import socket
import time
import logging
import protocol
from client import Client

logger = logging.getLogger(__name__)


class ClientHandler:
    """Handles a single client connection with proper TCP stream buffering."""
    
    def __init__(self, client: Client, server):
        """Initialize client handler."""
        self.client = client
        self.server = server
        
        # Set socket timeout for idle detection
        self.client.socket.settimeout(1.0)
    
    def send(self, message):
        """Send a message to the client using sendall() for reliability."""
        try:
            message_bytes = (message + '\r\n').encode('utf-8')
            self.client.socket.sendall(message_bytes)
            return True
        except Exception as e:
            logger.error(f"Error sending message to {self.client.address}: {e}")
            return False
    
    def read_lines(self):
        """Read complete lines from the socket, handling TCP stream framing.
        
        Returns a list of complete lines, or None if no data available or error.
        Handles cases where:
        - Multiple commands arrive in one recv()
        - A single command is split across multiple recv() calls
        - Empty data (client disconnect)
        """
        try:
            # Read available data
            chunk = self.client.socket.recv(1024).decode('utf-8')
            
            if not chunk:
                # Client disconnected
                return None
            
            # Update activity timestamp (both local and server)
            self.client.last_activity = time.time()
            if self.client.username:  # Only update if logged in
                self.server.update_user_activity(self.client.client_id)
            
            # Append to buffer
            self.client.buffer += chunk
            
            # Normalize line endings: convert \r\n to \n
            self.client.buffer = self.client.buffer.replace('\r\n', '\n')
            # Also handle standalone \r (old Mac style)
            self.client.buffer = self.client.buffer.replace('\r', '\n')
            
            # Process all complete lines in buffer
            lines = []
            while '\n' in self.client.buffer:
                line, self.client.buffer = self.client.buffer.split('\n', 1)
                line = line.strip()
                
                # Only add non-empty lines
                if line:
                    lines.append(line)
            
            # Return list of complete lines (empty list if no complete lines yet)
            # Empty list indicates we received data but no complete lines
            # None indicates timeout or error
            return lines
        
        except socket.timeout:
            # Timeout is normal, no data available yet
            return None
        except Exception as e:
            logger.error(f"Error reading from {self.client.address}: {e}")
            return None
    
    def handle_login(self):
        """Handle the login phase. Returns True if login successful, False otherwise."""
        while self.server.running.is_set():
            # Read complete lines
            lines = self.read_lines()
            if lines is None:
                # Timeout or error - check for idle timeout
                if time.time() - self.client.last_activity > self.server.idle_timeout:
                    return False
                continue
            
            # Process all complete lines received
            for line in lines:
                if not line:
                    continue
                
                # Skip very short lines that are likely telnet control characters or echo
                # Minimum valid command is "WHO" (3 chars) or "PING" (4 chars)
                if len(line) < 3:
                    continue
                
                # Parse LOGIN command
                username = protocol.parse_login_command(line)
                
                if username is not None:
                    # Validate username
                    if not username:
                        self.send(protocol.format_error(protocol.ERR_INVALID_USERNAME))
                        continue
                    
                    # Check if username is taken
                    if not self.server.register_user(self.client, username):
                        self.send(protocol.format_error(protocol.ERR_USERNAME_TAKEN))
                        continue
                    
                    # Login successful
                    self.send(protocol.RESP_OK)
                    logger.info(f"User '{username}' logged in from {self.client.address}")
                    
                    # Broadcast user joined
                    self.server.broadcast(
                        protocol.format_info(f"{username} connected"),
                        exclude_client_id=self.client.client_id
                    )
                    return True
                else:
                    # Not logged in yet, only accept LOGIN
                    self.send(protocol.format_error(protocol.ERR_NOT_LOGGED_IN))
        
        return False
    
    def handle_commands(self):
        """Handle commands after login. Processes commands until disconnect or logout.
        
        Returns True if logout was requested, False otherwise (disconnect/error).
        """
        last_activity_update = time.time()
        activity_update_interval = 10  # Update activity every 10 seconds to prevent idle timeout
        
        while self.server.running.is_set():
            # Read complete lines
            lines = self.read_lines()
            if lines is None:
                # Timeout is normal - but update activity periodically to show connection is alive
                current_time = time.time()
                if current_time - last_activity_update >= activity_update_interval:
                    # Update activity to show user is still connected (even if idle)
                    if self.client.username:
                        self.server.update_user_activity(self.client.client_id)
                    last_activity_update = current_time
                continue
            
            # Data received - update activity timestamp
            last_activity_update = time.time()
            
            # Process all complete lines received
            for line in lines:
                if not line:
                    continue
                
                # Parse and handle commands
                if line == protocol.CMD_LOGOUT:
                    # User requested logout
                    if self.send(protocol.RESP_OK):
                        logger.info(f"User '{self.client.username}' logged out from {self.client.address}")
                        return True
                    else:
                        logger.warning(f"Failed to send logout confirmation to {self.client.username}")
                        return False
                
                elif line.startswith(protocol.CMD_MSG + ' '):
                    # Broadcast message
                    message_text = protocol.parse_message_command(line)
                    if message_text:
                        self.server.broadcast(
                            protocol.format_message(self.client.username, message_text),
                            exclude_client_id=self.client.client_id
                        )
                
                elif line == protocol.CMD_WHO:
                    # List active users
                    self.server.send_user_list(self.client)
                
                elif line.startswith(protocol.CMD_DM + ' '):
                    # Private message
                    result = protocol.parse_dm_command(line)
                    if result:
                        target_username, message_text = result
                        logger.debug(f"DM from {self.client.username} to {target_username}: {message_text}")
                        self.server.send_private_message(
                            self.client.username, target_username, message_text
                        )
                    else:
                        logger.warning(f"Invalid DM format from {self.client.username}: {line}")
                        self.send(protocol.format_error(protocol.ERR_INVALID_DM_FORMAT))
                
                elif line == protocol.CMD_PING:
                    # Heartbeat
                    self.send(protocol.RESP_PONG)
                
                elif line:
                    # Unknown command
                    self.send(protocol.format_error(protocol.ERR_UNKNOWN_COMMAND))
        
        return False
    
    def disconnect(self):
        """Clean up and close the client connection."""
        try:
            self.client.socket.close()
        except Exception:
            pass
