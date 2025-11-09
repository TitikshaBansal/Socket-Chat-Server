#!/usr/bin/env python3
"""
Client data structure for tracking connected users.
"""

import socket
import threading
from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class Client:
    """Represents a connected client with all associated state."""
    client_id: str  # Opaque unique identifier
    socket: socket.socket
    address: tuple  # (host, port)
    username: Optional[str] = None
    thread: Optional[threading.Thread] = None
    buffer: str = ""
    last_activity: float = field(default_factory=time.time)
    
    def __hash__(self):
        """Make Client hashable for use as dict key."""
        return hash(self.client_id)
    
    def __eq__(self, other):
        """Compare clients by ID."""
        if not isinstance(other, Client):
            return False
        return self.client_id == other.client_id

