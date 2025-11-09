#!/usr/bin/env python3
"""
TCP Chat Server
A socket-based chat server that allows multiple users to connect, log in, and exchange messages.
Uses only Python standard library (socket, threading, os, sys, argparse).
"""

import socket
import threading
import os
import sys
import argparse
import time
from collections import defaultdict


class ChatServer:
    """Main chat server class handling connections and messaging."""
    
    def __init__(self, port=4000):
        """Initialize the chat server with specified port."""
        self.port = port
        self.host = '0.0.0.0'  # Listen on all interfaces
        self.socket = None
        
        # Thread-safe data structures
        self.users = {}  # {socket: username}
        self.usernames = set()  # Set of active usernames
        self.user_last_activity = {}  # {socket: timestamp} for idle timeout
        self.lock = threading.Lock()  # Lock for thread-safe operations
        
        # Configuration
        self.idle_timeout = 60  # seconds
        self.max_users = 10
    
    def start(self):
        """Start the server and begin accepting connections."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
            self.socket.listen(self.max_users)
            print(f"Chat server started on port {self.port}")
            print(f"Listening for up to {self.max_users} concurrent connections...")
            
            # Start idle timeout checker thread
            timeout_thread = threading.Thread(target=self._check_idle_timeouts, daemon=True)
            timeout_thread.start()
            
            # Accept connections in a loop
            while True:
                client_socket, address = self.socket.accept()
                print(f"New connection from {address}")
                
                # Check if we've reached max users
                with self.lock:
                    if len(self.users) >= self.max_users:
                        client_socket.send(b"ERR server-full\n")
                        client_socket.close()
                        continue
                
                # Handle each client in a separate thread
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
                
        except OSError as e:
            print(f"Error starting server: {e}")
            sys.exit(1)
        except KeyboardInterrupt:
            print("\nShutting down server...")
            self.shutdown()
    
    def _handle_client(self, client_socket, address):
        """Handle communication with a single client."""
        username = None
        
        try:
            # Set socket timeout for idle detection
            client_socket.settimeout(1.0)
            
            # Login phase
            while True:
                try:
                    data = client_socket.recv(1024).decode('utf-8')
                    if not data:
                        break
                    
                    # Update activity timestamp
                    with self.lock:
                        self.user_last_activity[client_socket] = time.time()
                    
                    # Parse command
                    command = data.strip()
                    
                    if command.startswith('LOGIN '):
                        username = command[6:].strip()
                        
                        # Validate username
                        if not username:
                            client_socket.send(b"ERR invalid-username\n")
                            continue
                        
                        # Check if username is taken
                        with self.lock:
                            if username in self.usernames:
                                client_socket.send(b"ERR username-taken\n")
                                continue
                            
                            # Register user
                            self.users[client_socket] = username
                            self.usernames.add(username)
                            self.user_last_activity[client_socket] = time.time()
                        
                        client_socket.send(b"OK\n")
                        print(f"User '{username}' logged in from {address}")
                        
                        # Broadcast user joined (optional enhancement)
                        self._broadcast(f"INFO {username} connected", exclude=client_socket)
                        break
                    else:
                        # Not logged in yet, only accept LOGIN
                        client_socket.send(b"ERR not-logged-in\n")
                        
                except socket.timeout:
                    # Check for idle timeout during login
                    with self.lock:
                        if client_socket in self.user_last_activity:
                            elapsed = time.time() - self.user_last_activity[client_socket]
                            if elapsed > self.idle_timeout:
                                break
                    continue
                except Exception as e:
                    print(f"Error during login: {e}")
                    break
            
            # If login failed, close connection
            if not username:
                client_socket.close()
                return
            
            # Main message handling loop
            while True:
                try:
                    data = client_socket.recv(1024).decode('utf-8')
                    if not data:
                        break
                    
                    # Update activity timestamp
                    with self.lock:
                        if client_socket in self.user_last_activity:
                            self.user_last_activity[client_socket] = time.time()
                    
                    # Parse and handle commands
                    command = data.strip()
                    
                    if command.startswith('MSG '):
                        # Broadcast message
                        message_text = command[4:].strip()
                        if message_text:
                            self._broadcast(f"MSG {username} {message_text}", exclude=client_socket)
                    
                    elif command == 'WHO':
                        # List active users
                        self._send_user_list(client_socket)
                    
                    elif command.startswith('DM '):
                        # Private message
                        parts = command[3:].strip().split(' ', 1)
                        if len(parts) == 2:
                            target_username, message_text = parts
                            self._send_private_message(username, target_username, message_text)
                        else:
                            client_socket.send(b"ERR invalid-dm-format\n")
                    
                    elif command == 'PING':
                        # Heartbeat
                        client_socket.send(b"PONG\n")
                    
                    elif command:
                        # Unknown command
                        client_socket.send(b"ERR unknown-command\n")
                    
                except socket.timeout:
                    # Timeout is normal, continue loop to check for idle
                    continue
                except Exception as e:
                    print(f"Error handling message from {username}: {e}")
                    break
        
        except Exception as e:
            print(f"Error in client handler for {address}: {e}")
        
        finally:
            # Clean up on disconnect
            self._disconnect_user(client_socket, username)
    
    def _broadcast(self, message, exclude=None):
        """Broadcast a message to all connected users except the sender."""
        message_bytes = (message + '\n').encode('utf-8')
        disconnected = []
        
        with self.lock:
            for client_socket in list(self.users.keys()):
                if client_socket != exclude:
                    try:
                        client_socket.send(message_bytes)
                    except Exception:
                        # Socket is closed, mark for removal
                        disconnected.append(client_socket)
        
        # Remove disconnected clients
        for client_socket in disconnected:
            self._disconnect_user(client_socket, None)
    
    def _send_private_message(self, sender_username, target_username, message_text):
        """Send a private message from one user to another."""
        with self.lock:
            # Find target user's socket
            target_socket = None
            for socket_obj, username in self.users.items():
                if username == target_username:
                    target_socket = socket_obj
                    break
            
            if target_socket:
                try:
                    message = f"DM {sender_username} {message_text}\n"
                    target_socket.send(message.encode('utf-8'))
                except Exception:
                    # Target disconnected, remove them
                    self._disconnect_user(target_socket, target_username)
            else:
                # User not found, notify sender
                sender_socket = None
                for socket_obj, username in self.users.items():
                    if username == sender_username:
                        sender_socket = socket_obj
                        break
                
                if sender_socket:
                    try:
                        sender_socket.send(f"ERR user-not-found {target_username}\n".encode('utf-8'))
                    except Exception:
                        pass
    
    def _send_user_list(self, client_socket):
        """Send list of active users to a client."""
        with self.lock:
            usernames_list = sorted(self.usernames)
        
        for username in usernames_list:
            try:
                client_socket.send(f"USER {username}\n".encode('utf-8'))
            except Exception:
                return
    
    def _disconnect_user(self, client_socket, username):
        """Remove a user from the server and broadcast disconnect message."""
        with self.lock:
            if client_socket in self.users:
                if username is None:
                    username = self.users[client_socket]
                
                del self.users[client_socket]
                if username in self.usernames:
                    self.usernames.remove(username)
                if client_socket in self.user_last_activity:
                    del self.user_last_activity[client_socket]
        
        try:
            client_socket.close()
        except Exception:
            pass
        
        if username:
            print(f"User '{username}' disconnected")
            self._broadcast(f"INFO {username} disconnected")
    
    def _check_idle_timeouts(self):
        """Background thread to check for idle users and disconnect them."""
        while True:
            time.sleep(5)  # Check every 5 seconds
            current_time = time.time()
            idle_clients = []
            
            with self.lock:
                for client_socket, last_activity in list(self.user_last_activity.items()):
                    elapsed = current_time - last_activity
                    if elapsed > self.idle_timeout:
                        idle_clients.append(client_socket)
            
            # Disconnect idle clients
            for client_socket in idle_clients:
                self._disconnect_user(client_socket, None)
    
    def shutdown(self):
        """Gracefully shutdown the server."""
        if self.socket:
            self.socket.close()
        sys.exit(0)


def main():
    """Main entry point for the chat server."""
    parser = argparse.ArgumentParser(description='TCP Chat Server')
    parser.add_argument(
        '--port', '-p',
        type=int,
        default=None,
        help='Port number to listen on (default: 4000 or PORT environment variable)'
    )
    
    args = parser.parse_args()
    
    # Determine port: command-line argument > environment variable > default
    port = args.port
    if port is None:
        port = int(os.environ.get('PORT', 4000))
    
    # Validate port
    if not (1 <= port <= 65535):
        print(f"Error: Port must be between 1 and 65535")
        sys.exit(1)
    
    # Create and start server
    server = ChatServer(port=port)
    server.start()


if __name__ == '__main__':
    main()

