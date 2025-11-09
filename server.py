#!/usr/bin/env python3
"""
Main chat server class handling connections and user management.
"""

import socket
import threading
import sys
import time
import logging
import uuid
from typing import Optional
import client_handler
import protocol
from client import Client


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ChatServer:
    """Main chat server class handling connections and messaging."""
    
    def __init__(self, port=4000):
        """Initialize the chat server with specified port."""
        self.port = port
        self.host = '0.0.0.0'  # Listen on all interfaces
        self.socket = None
        self.running = threading.Event()
        self.running.set()  # Server starts as running
        
        # Thread-safe data structures - using client_id as key
        self.clients = {}  # {client_id: Client}
        self.clients_by_username = {}  # {username: client_id}
        self.clients_by_socket = {}  # {socket: client_id} for quick lookup
        self.lock = threading.Lock()  # Lock for thread-safe operations
        
        # Background threads
        self.client_threads = []  # List of client handler threads
        self.timeout_thread = None
        
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
            logger.info(f"Chat server started on port {self.port}")
            logger.info(f"Listening for up to {self.max_users} concurrent connections...")
            
            # Start idle timeout checker thread
            self.timeout_thread = threading.Thread(target=self._check_idle_timeouts, daemon=True)
            self.timeout_thread.start()
            
            # Accept connections in a loop
            while self.running.is_set():
                try:
                    self.socket.settimeout(1.0)  # Allow checking running flag
                    client_socket, address = self.socket.accept()
                    logger.info(f"New connection from {address}")
                    
                    # Check if we've reached max users
                    with self.lock:
                        if len(self.clients) >= self.max_users:
                            try:
                                client_socket.sendall(b"ERR server-full\n")
                            except Exception:
                                pass
                            client_socket.close()
                            logger.warning(f"Connection rejected from {address}: server full")
                            continue
                    
                    # Create client object
                    client_id = str(uuid.uuid4())
                    client = Client(
                        client_id=client_id,
                        socket=client_socket,
                        address=address
                    )
                    
                    # Handle each client in a separate thread
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client,),
                        daemon=False  # Not daemon so we can wait for them
                    )
                    client.thread = client_thread
                    self.client_threads.append(client_thread)
                    client_thread.start()
                    
                except socket.timeout:
                    # Timeout is expected, continue loop to check running flag
                    continue
                except OSError as e:
                    if self.running.is_set():
                        logger.error(f"Error accepting connection: {e}")
                    break
                
        except OSError as e:
            logger.error(f"Error starting server: {e}")
            sys.exit(1)
        except KeyboardInterrupt:
            logger.info("\nShutting down server...")
            self.shutdown()
    
    def _handle_client(self, client: Client):
        """Handle communication with a single client."""
        handler = client_handler.ClientHandler(client, self)
        
        try:
            # Login phase
            if not handler.handle_login():
                # Login failed or timeout
                handler.disconnect()
                return
            
            # Main command handling phase
            logout_requested = handler.handle_commands()
            # If logout was requested, handle_commands returns True
            # Otherwise, it runs until disconnect/error
        
        except Exception as e:
            logger.error(f"Error in client handler for {client.address}: {e}", exc_info=True)
        
        finally:
            # Clean up on disconnect or logout
            # disconnect_user will broadcast the disconnect message
            self.disconnect_user(client.client_id)
    
    def register_user(self, client: Client, username: str):
        """Register a new user. Returns True if successful, False if username taken."""
        with self.lock:
            if username in self.clients_by_username:
                return False
            
            # Register user
            client.username = username
            self.clients[client.client_id] = client
            self.clients_by_username[username] = client.client_id
            self.clients_by_socket[client.socket] = client.client_id
            client.last_activity = time.time()
            return True
    
    def get_client_by_id(self, client_id: str) -> Optional[Client]:
        """Get client by ID."""
        with self.lock:
            return self.clients.get(client_id)
    
    def get_client_by_username(self, username: str) -> Optional[Client]:
        """Get client by username."""
        with self.lock:
            client_id = self.clients_by_username.get(username)
            if client_id:
                return self.clients.get(client_id)
            return None
    
    def update_user_activity(self, client_id: str):
        """Update the last activity timestamp for a user."""
        with self.lock:
            client = self.clients.get(client_id)
            if client:
                client.last_activity = time.time()
    
    def broadcast(self, message, exclude_client_id=None):
        """Broadcast a message to all connected users except the sender."""
        message_bytes = (message + '\n').encode('utf-8')
        disconnected = []
        
        with self.lock:
            for client_id, client in list(self.clients.items()):
                if client_id != exclude_client_id:
                    try:
                        client.socket.sendall(message_bytes)
                    except Exception:
                        # Socket is closed, mark for removal
                        disconnected.append(client_id)
        
        # Remove disconnected clients
        for client_id in disconnected:
            self.disconnect_user(client_id)
    
    def send_private_message(self, sender_username, target_username, message_text):
        """Send a private message from one user to another."""
        # Find clients while holding lock, then release before sending
        with self.lock:
            target_client = self.get_client_by_username(target_username)
            sender_client = self.get_client_by_username(sender_username)
        
        if target_client:
            try:
                message = protocol.format_private_message(sender_username, message_text)
                target_client.socket.sendall((message + '\n').encode('utf-8'))
            except (BrokenPipeError, ConnectionError, OSError) as e:
                # Only disconnect on actual socket errors, not all exceptions
                logger.warning(f"Error sending DM to {target_username}: {e}")
                # Target disconnected, remove them
                self.disconnect_user(target_client.client_id)
            except Exception as e:
                # Other exceptions - log but don't disconnect
                logger.error(f"Unexpected error sending DM to {target_username}: {e}", exc_info=True)
        else:
            # User not found, notify sender
            if sender_client:
                try:
                    error_msg = protocol.format_error(
                        protocol.ERR_USER_NOT_FOUND, target_username
                    )
                    sender_client.socket.sendall((error_msg + '\n').encode('utf-8'))
                except Exception:
                    pass
    
    def send_user_list(self, client: Client):
        """Send list of active users to a client."""
        with self.lock:
            usernames_list = sorted(self.clients_by_username.keys())
        
        for username in usernames_list:
            try:
                message = protocol.format_user_list(username)
                client.socket.sendall((message + '\n').encode('utf-8'))
            except Exception:
                return
    
    def disconnect_user(self, client_id: str):
        """Remove a user from the server and broadcast disconnect message."""
        client = None
        username = None
        
        with self.lock:
            client = self.clients.get(client_id)
            if client:
                username = client.username
                
                # Remove from all dictionaries
                if client_id in self.clients:
                    del self.clients[client_id]
                if username and username in self.clients_by_username:
                    del self.clients_by_username[username]
                if client.socket in self.clients_by_socket:
                    del self.clients_by_socket[client.socket]
        
        if client:
            try:
                client.socket.close()
            except Exception:
                pass
        
        if username:
            logger.info(f"User '{username}' disconnected")
            self.broadcast(protocol.format_info(f"{username} disconnected"), exclude_client_id=client_id)
    
    def _check_idle_timeouts(self):
        """Background thread to check for idle users and disconnect them."""
        while self.running.is_set():
            time.sleep(5)  # Check every 5 seconds
            current_time = time.time()
            idle_clients = []
            
            with self.lock:
                for client_id, client in list(self.clients.items()):
                    elapsed = current_time - client.last_activity
                    if elapsed > self.idle_timeout:
                        logger.info(f"Idle timeout: {client.username} inactive for {elapsed:.1f} seconds")
                        idle_clients.append(client_id)
            
            # Disconnect idle clients
            for client_id in idle_clients:
                self.disconnect_user(client_id)
    
    def shutdown(self):
        """Gracefully shutdown the server."""
        logger.info("Initiating graceful shutdown...")
        self.running.clear()
        
        # Close listener socket
        if self.socket:
            try:
                self.socket.close()
                logger.info("Listener socket closed")
            except Exception as e:
                logger.error(f"Error closing listener socket: {e}")
        
        # Disconnect all clients
        with self.lock:
            client_ids = list(self.clients.keys())
        
        logger.info(f"Disconnecting {len(client_ids)} clients...")
        for client_id in client_ids:
            self.disconnect_user(client_id)
        
        # Wait for client threads to finish (with timeout)
        logger.info("Waiting for client threads to finish...")
        for thread in self.client_threads[:]:  # Copy list to avoid modification during iteration
            if thread.is_alive():
                thread.join(timeout=2.0)  # Wait up to 2 seconds per thread
                if thread.is_alive():
                    logger.warning(f"Client thread {thread.name} did not finish in time")
        
        logger.info("Server shutdown complete")
        sys.exit(0)
