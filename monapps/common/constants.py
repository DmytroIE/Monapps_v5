from django.db import models


STATUS_FIELD_NAME = "Status"
CURR_STATE_FIELD_NAME = "Current state"


class StatusTypes(models.IntegerChoices):
    UNDEFINED = 0
    OK = 1
    WARNING = 2
    ERROR = 3


class CurrStateTypes(models.IntegerChoices):
    UNDEFINED = 0
    OK = 1
    WARNING = 2
    ERROR = 3


class StatusUse(models.IntegerChoices):
    DONT_USE = 0
    AS_IS = 1
    AS_WARNING = 2
    AS_ERROR_IF_ALL = 3


class CurrStateUse(models.IntegerChoices):
    DONT_USE = 0
    AS_IS = 1
    AS_WARNING = 2
    AS_ERROR_IF_ALL = 3


class HealthGrades(models.IntegerChoices):
    UNDEFINED = 0
    OK = 1
    WARNING = 2
    ERROR = 3


# https://inforiver.com/insights/continuous-discrete-categorical-axis-difference/
class VariableTypes(models.IntegerChoices):
    CONTINUOUS = 0
    DISCRETE = 1
    NOMINAL = 3  # categorical
    ORDINAL = 4  # categorical


class DataAggTypes(models.IntegerChoices):
    AVG = 0  # not available for categorical and discrete data
    SUM = 1  # not available for categorical data
    LAST = 2  # can be used for cat. data that represents a certain state
    # MAX = 3  # not available for categorical data
    # MIN = 4  # not available for categorical data
    # MODE = 5  # for categorical data only


class NotToUseDfrTypes(models.IntegerChoices):
    SPLINE_NOT_TO_USE = 1
    UNCLOSED = 2
    SPLINE_UNCLOSED = 3


class AugmentationPolicy(models.IntegerChoices):
    TILL_LAST_DF_READING = 1
    TILL_NOW = 2


class AllowedIntervalsMs(models.IntegerChoices):
    ONE_SEC = 1000
    FIVE_SECS = 5000
    TEN_SECS = 10000
    HALF_MIN = 30000
    MIN = 60000
    FIVE_MIN = 300000
    TEN_MIN = 600000
    HALF_HOUR = 1800000
    HOUR = 3600000
    DAY = 86400000


DEFAULT_TIME_RESAMPLE = AllowedIntervalsMs.MIN
DEFAULT_TIME_STATUS_STALE = AllowedIntervalsMs.DAY * 15
DEFAULT_TIME_CURR_STATE_STALE = AllowedIntervalsMs.TEN_MIN
DEFAULT_TIME_APP_HEALTH_ERROR = AllowedIntervalsMs.TEN_MIN


class DfTypes(models.TextChoices):
    NONE = "None"
    STATUS = "Status"  # will be reflected as "stepped" in the frontend, should be NOMINAL/ORDINAL
    CURRENT_STATE = "Current state"  # will be reflected as "stepped" in the frontend, should be NOMINAL/ORDINAL
    STATE = "State"  # will be reflected as "stepped" in the frontend, should be NOMINAL/ORDINAL
    STEAM_LOSS = "Steam loss"
    WATER_LOSS = "Water loss"
    ENERGY_LOSS = "Energy loss"
    STEAM_SAVINGS = "Steam savings"
    WATER_SAVINGS = "Water savings"
    ENERGY_SAVINGS = "Energy savings"


class AppPurps(models.TextChoices):  # application purpose
    NONE = "None"
    MONITORING = "Monitoring"
    LEAKING = "Leaking"
    BLOCKED = "Blocked"
    FLOODED = "Flooded"
    BROKEN = "Broken"
    MALFUNCTIONING = "Malfunctioning"


class AssetTypes(models.TextChoices):
    GENERIC = "Generic"
    SITE = "Site"
    WORKSHOP = "Workshop"
    BOILER = "BOILER"
    DEAERATOR = "DEAERATOR"
    STEAM_TRAP = "Steam trap"
    HEAT_EX = "Heat exchanger"
    HEAT_PACKAGE = "Heating package"
    CONTROL_VALVE = "Control valve"
    PRS = "PRS"
    SAFETY_VALVE = "Safety valve"
    MECH_PUMP = "Mechanical pump"
    MECH_PUMP_CRU = "Mechanical pump CRU"
    EL_PUMP = "Electrical pump"
    EL_PUMP_CRU = "Electrical pump CRU"


class DataTypes(models.TextChoices):
    TEMP = "Temperature"
    PRES = "Pressure"
    MASS = "Mass"
    MASS_FLOW = "Mass flowrate"
    VOLUM_FLOW = "Volumetric flowrate"
    COND = "Conductivity"
    STATE = "State"
    COUNTS = "Counts"


class MeasUnits(models.TextChoices):
    DIMENSIONLESS = "-"
    DEG_C = "*C"
    BARG = "barg"
    KPA = "kPa"
    KG = "kg"
    KG_H = "kg/h"
    KG_S = "kg/s"
    M3_H = "m3/h"
    M3_S = "m3/s"
    USM_CM = "uSm/cm"


reeval_fields = {"status", "curr_state", "health"}
