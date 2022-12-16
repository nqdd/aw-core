import logging
from typing import List, Dict, Tuple

from aw_core.models import Event

logger = logging.getLogger(__name__)


def merge_events(events: List[Event]) -> List[Event]:
    """
    Traverse all events and merge event that have overlap.

    .. note: The result will be a list of events.
    .. NOTE: Only use to merge events of the same type (afk, window,...) with the same data
    """
    merged_events = []
    events.sort(key= lambda event: event.timestamp )
    if not events:
        return []
    cur_e_i = 0
    while cur_e_i < len(events) - 1:
        cur_event = events[cur_e_i]
        next_event = events[cur_e_i + 1]
        # next_event start before cur_event end
        if cur_event.timestamp + cur_event.duration > next_event.timestamp:
            new_duration = max(cur_event.duration, next_event.timestamp + next_event.duration - cur_event.timestamp)
            new_event = Event(id=cur_event.id,timestamp=cur_event.timestamp, duration=new_duration,data=cur_event.data)
            events[cur_e_i] = new_event
            # remove next_event and continue checking for further event
            events.remove(next_event)
        # next_event start after cur_event end
        else:
            merged_events.append(cur_event)
            cur_e_i += 1

    merged_events.append(events[len(events) - 1])
    return merged_events
        
    