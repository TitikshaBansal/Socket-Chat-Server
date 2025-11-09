#!/usr/bin/env python3
"""
Protocol definitions and message formatting for the chat server.
"""

# Command prefixes
CMD_LOGIN = 'LOGIN'
CMD_MSG = 'MSG'
CMD_WHO = 'WHO'
CMD_DM = 'DM'
CMD_PING = 'PING'

# Response prefixes
RESP_OK = 'OK'
RESP_PONG = 'PONG'
RESP_ERR = 'ERR'
RESP_INFO = 'INFO'
RESP_USER = 'USER'

# Error codes
ERR_USERNAME_TAKEN = 'username-taken'
ERR_NOT_LOGGED_IN = 'not-logged-in'
ERR_INVALID_USERNAME = 'invalid-username'
ERR_INVALID_DM_FORMAT = 'invalid-dm-format'
ERR_USER_NOT_FOUND = 'user-not-found'
ERR_UNKNOWN_COMMAND = 'unknown-command'
ERR_SERVER_FULL = 'server-full'


def format_message(username, text):
    """Format a broadcast message."""
    return f"{CMD_MSG} {username} {text}"


def format_private_message(sender, text):
    """Format a private message."""
    return f"{CMD_DM} {sender} {text}"


def format_user_list(username):
    """Format a user list entry."""
    return f"{RESP_USER} {username}"


def format_info(message):
    """Format an info message."""
    return f"{RESP_INFO} {message}"


def format_error(error_code, details=''):
    """Format an error message."""
    if details:
        return f"{RESP_ERR} {error_code} {details}"
    return f"{RESP_ERR} {error_code}"


def parse_login_command(command):
    """Parse LOGIN command and return username."""
    if not command.startswith(f"{CMD_LOGIN} "):
        return None
    return command[len(CMD_LOGIN) + 1:].strip()


def parse_message_command(command):
    """Parse MSG command and return message text."""
    if not command.startswith(f"{CMD_MSG} "):
        return None
    return command[len(CMD_MSG) + 1:].strip()


def parse_dm_command(command):
    """Parse DM command and return (target_username, message_text) tuple."""
    if not command.startswith(f"{CMD_DM} "):
        return None
    parts = command[len(CMD_DM) + 1:].strip().split(' ', 1)
    if len(parts) != 2:
        return None
    return parts[0].strip(), parts[1].strip()

