#!/usr/bin/env python3
"""
Client handler for managing individual client connections.
Handles TCP stream buffering, command parsing, and message sending.
"""

import socket
import time
import protocol


class ClientHandler:
    """Handles a single client connection with proper TCP stream buffering."""
    
    def __init__(self, client_socket, address, server):
        """Initialize client handler."""
        self.client_socket = client_socket
        self.address = address
        self.server = server
        self.username = None
        self.buffer = ""
        self.last_activity = time.time()
        
        # Set socket timeout for idle detection
        self.client_socket.settimeout(1.0)
    
    def send(self, message):
        """Send a message to the client using sendall() for reliability."""
        try:
            message_bytes = (message + '\n').encode('utf-8')
            self.client_socket.sendall(message_bytes)
            return True
        except Exception as e:
            print(f"Error sending message to {self.address}: {e}")
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
            chunk = self.client_socket.recv(1024).decode('utf-8')
            
            if not chunk:
                # Client disconnected
                return None
            
            # Update activity timestamp (both local and server)
            self.last_activity = time.time()
            if self.username:  # Only update if logged in
                self.server.update_user_activity(self.client_socket)
            
            # Append to buffer
            self.buffer += chunk
            
            # Normalize line endings: convert \r\n to \n
            self.buffer = self.buffer.replace('\r\n', '\n')
            # Also handle standalone \r (old Mac style)
            self.buffer = self.buffer.replace('\r', '\n')
            
            # Process all complete lines in buffer
            lines = []
            while '\n' in self.buffer:
                line, self.buffer = self.buffer.split('\n', 1)
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
            print(f"Error reading from {self.address}: {e}")
            return None
    
    def handle_login(self):
        """Handle the login phase. Returns True if login successful, False otherwise."""
        while True:
            # Read complete lines
            lines = self.read_lines()
            if lines is None:
                # Timeout or error - check for idle timeout
                if time.time() - self.last_activity > self.server.idle_timeout:
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
                    if not self.server.register_user(self.client_socket, username):
                        self.send(protocol.format_error(protocol.ERR_USERNAME_TAKEN))
                        continue
                    
                    # Login successful
                    self.username = username
                    self.send(protocol.RESP_OK)
                    print(f"User '{username}' logged in from {self.address}")
                    
                    # Broadcast user joined
                    self.server.broadcast(
                        protocol.format_info(f"{username} connected"),
                        exclude=self.client_socket
                    )
                    return True
                else:
                    # Not logged in yet, only accept LOGIN
                    self.send(protocol.format_error(protocol.ERR_NOT_LOGGED_IN))
    
    def handle_commands(self):
        """Handle commands after login. Processes commands until disconnect."""
        while True:
            # Read complete lines
            lines = self.read_lines()
            if lines is None:
                # Timeout is normal, continue loop (idle timeout handled by server)
                continue
            
            # Process all complete lines received
            for line in lines:
                if not line:
                    continue
                
                # Parse and handle commands
                if line.startswith(protocol.CMD_MSG + ' '):
                    # Broadcast message
                    message_text = protocol.parse_message_command(line)
                    if message_text:
                        self.server.broadcast(
                            protocol.format_message(self.username, message_text),
                            exclude=self.client_socket
                        )
                
                elif line == protocol.CMD_WHO:
                    # List active users
                    self.server.send_user_list(self.client_socket)
                
                elif line.startswith(protocol.CMD_DM + ' '):
                    # Private message
                    result = protocol.parse_dm_command(line)
                    if result:
                        target_username, message_text = result
                        self.server.send_private_message(
                            self.username, target_username, message_text
                        )
                    else:
                        self.send(protocol.format_error(protocol.ERR_INVALID_DM_FORMAT))
                
                elif line == protocol.CMD_PING:
                    # Heartbeat
                    self.send(protocol.RESP_PONG)
                
                elif line:
                    # Unknown command
                    self.send(protocol.format_error(protocol.ERR_UNKNOWN_COMMAND))
    
    def disconnect(self):
        """Clean up and close the client connection."""
        try:
            self.client_socket.close()
        except Exception:
            pass

