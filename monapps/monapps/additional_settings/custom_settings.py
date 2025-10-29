# Monitoring Application settings
NUM_MAX_DFREADINGS_TO_PROCESS = 50000
NUM_MAX_DSREADINGS_TO_PROCESS = 100000
MIN_TIME_RESOL_MS = 1000
MIN_TIME_APP_FUNC_INVOC_MS = 60000

# DS health monitoring settings
MAX_DS_TO_HEALTH_PROC = 100
TIME_DS_HEALTH_EVAL_MS = 5000  # 5 seconds, how often the ds health check procedure is executed
NEXT_EVAL_MARGIN_COEF = 1.5  # will be a multiplicator for 'time_update' for periodic datastreams

# Asset/Device update settings
# 10 seconds, how often the asset/device update procedure is executed,
# should match the interval duration
TIME_ASSET_UPD_MS = 10000

MAX_ASSETS_TO_UPD = 100
MAX_DEVICES_TO_UPD = 50

# it is datetime(2999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc), something similar to Infinity
MAX_TS_MS = 32503679999999

# to ensure that the asset is updated from time to time even if the children haven't initiated an update
TIME_DELAY_ASSET_MANDATORY_UPDATE_MS = 60000 * 120  # 2 hours

# API settings
MAX_READINGS_PER_API_CALL = 1000
