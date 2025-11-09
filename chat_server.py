#!/usr/bin/env python3
"""
TCP Chat Server - Main Entry Point
A socket-based chat server that allows multiple users to connect, log in, and exchange messages.
Uses only Python standard library (socket, threading, os, sys, argparse).
"""

import os
import sys
import argparse
import server


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
    chat_server = server.ChatServer(port=port)
    chat_server.start()


if __name__ == '__main__':
    main()

