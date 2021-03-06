import threading
from ..messaging import generate_message

# Default state shared between threads:
shared_state = {
    'params': {
        'cpu_pause_ms': 20,
        'message_bytes': 200,
        'period_sec': 1.0,
    }
}


# TODO: use 'frequency' instead of period. (make it an integer)

def regenerate_data():
    shared_state['message'], filename = generate_message(shared_state['params'])


regenerate_data()

shared_state_lock = threading.Lock()
