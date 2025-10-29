from .stall_detection_by_two_temps import stall_detection_by_two_temps_0_0_1, stall_detection_by_two_temps_1_0_0
from .sv_leak_detection_by_two_temps import sv_leak_detection_by_two_temps_1_0_0
from .monitoring import monitoring_1_0_0
from .fake_data_generator import fake_data_generator_1_0_0


app_function_map = {
    "stall_detection_by_two_temps": {
        "0.0.1": stall_detection_by_two_temps_0_0_1,
        "1.0.0": stall_detection_by_two_temps_1_0_0,
    },
    "sv_leak_detection_by_two_temps": {
        "1.0.0": sv_leak_detection_by_two_temps_1_0_0,
    },
    "monitoring": {
        "1.0.0": monitoring_1_0_0,
    },
    "fake_data_generator": {
        "1.0.0": fake_data_generator_1_0_0,
    },
}
