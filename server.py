#!/usr/bin/env python3
"""
Main chat server class handling connections and user management.
"""

import socket
import threading
import sys
import time
import client_handler
import protocol


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
                        try:
                            client_socket.sendall(b"ERR server-full\n")
                        except Exception:
                            pass
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
        handler = client_handler.ClientHandler(client_socket, address, self)
        
        try:
            # Login phase
            if not handler.handle_login():
                # Login failed or timeout
                handler.disconnect()
                return
            
            # Main command handling phase
            handler.handle_commands()
        
        except Exception as e:
            print(f"Error in client handler for {address}: {e}")
        
        finally:
            # Clean up on disconnect
            self.disconnect_user(client_socket, handler.username)
    
    def register_user(self, client_socket, username):
        """Register a new user. Returns True if successful, False if username taken."""
        with self.lock:
            if username in self.usernames:
                return False
            
            # Register user
            self.users[client_socket] = username
            self.usernames.add(username)
            self.user_last_activity[client_socket] = time.time()
            return True
    
    def broadcast(self, message, exclude=None):
        """Broadcast a message to all connected users except the sender."""
        message_bytes = (message + '\n').encode('utf-8')
        disconnected = []
        
        with self.lock:
            for client_socket in list(self.users.keys()):
                if client_socket != exclude:
                    try:
                        client_socket.sendall(message_bytes)
                    except Exception:
                        # Socket is closed, mark for removal
                        disconnected.append(client_socket)
        
        # Remove disconnected clients
        for client_socket in disconnected:
            self.disconnect_user(client_socket, None)
    
    def send_private_message(self, sender_username, target_username, message_text):
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
                    message = protocol.format_private_message(sender_username, message_text)
                    target_socket.sendall((message + '\n').encode('utf-8'))
                except Exception:
                    # Target disconnected, remove them
                    self.disconnect_user(target_socket, target_username)
            else:
                # User not found, notify sender
                sender_socket = None
                for socket_obj, username in self.users.items():
                    if username == sender_username:
                        sender_socket = socket_obj
                        break
                
                if sender_socket:
                    try:
                        error_msg = protocol.format_error(
                            protocol.ERR_USER_NOT_FOUND, target_username
                        )
                        sender_socket.sendall((error_msg + '\n').encode('utf-8'))
                    except Exception:
                        pass
    
    def send_user_list(self, client_socket):
        """Send list of active users to a client."""
        with self.lock:
            usernames_list = sorted(self.usernames)
        
        for username in usernames_list:
            try:
                message = protocol.format_user_list(username)
                client_socket.sendall((message + '\n').encode('utf-8'))
            except Exception:
                return
    
    def disconnect_user(self, client_socket, username):
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
            self.broadcast(protocol.format_info(f"{username} disconnected"))
    
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
                self.disconnect_user(client_socket, None)
    
    def shutdown(self):
        """Gracefully shutdown the server."""
        if self.socket:
            self.socket.close()
        sys.exit(0)

