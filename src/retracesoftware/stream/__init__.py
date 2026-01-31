"""
retracesoftware_stream - Runtime selectable release/debug builds

Set RETRACE_DEBUG=1 to use the debug build with symbols and assertions.
"""
import os

_debug_mode = os.environ.get('RETRACE_DEBUG', '').lower() in ('1', 'true', 'yes')

if _debug_mode:
    from retracesoftware_stream.retracesoftware_stream_debug import *
    from retracesoftware_stream.retracesoftware_stream_debug import __doc__
else:
    from retracesoftware_stream.retracesoftware_stream_release import *
    from retracesoftware_stream.retracesoftware_stream_release import __doc__

# Expose which mode is active
DEBUG_MODE = _debug_mode
