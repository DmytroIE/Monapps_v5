/**
 * Decode uplink function
 * 
 * @param {object} input
 * @param {number[]} input.bytes Byte array containing the uplink payload, e.g. [255, 230, 255, 0]
 * @param {number} input.fPort Uplink fPort.
 * @param {Record<string, string>} input.variables Object containing the configured device variables.
 * 
 * @returns {{data: object}} Object representing the decoded payload.
 */


var _rx_units = {
    temp1: { scale: 0.1, signed: true },
    temp2: { scale: 0.1, signed: true },
    humidity: { scale: 0.1 },
    voc: {},
    co2: {},
    pulse_ch1: { length: 4 },
    pulse_ch2: { length: 4 },
    pulse_oc: { length: 4 },
    current: { scale: 0.001 },
};
var rx_units = _rx_units;
var rx = [
    [],
    [],
    [],
    [],
    ["temp1", "humidity"],
    ["temp1", "humidity", "voc"],
    ["temp1", "humidity", "voc", "co2"],
    ["temp1"],
    ["pulse_ch1", "pulse_ch2", "pulse_oc"],
    ["pulse_ch1", "pulse_ch2", "pulse_oc"],
    ["pulse_ch1", "pulse_ch2", "pulse_oc"],
    ["pulse_ch1", "pulse_ch2", "pulse_oc"],
    ["temp1", "temp2"],
    ["current"],
    ["temp1", "humidity"],
    ["temp1"],
    ["temp1"],
    ["temp1", "temp2"],
];

function readBytes(offset, size, bytes) {
    var value = 0;
    for (var i = 0; i < size; i++) {
        value <<= 8;
        value += bytes[offset + i];
    }
    return value;
};

function decodeUplink(input) {
    var bytes = input.bytes;
    var fPort = input.fPort;
    var variables = input.variables;
    if (!bytes || bytes.length === 0) {
        return ({ data: {} });
    }

    // var transmitterId = readBytes(0, 3);
    var sensorType = readBytes(3, 1, bytes);
    var payload = {};
    var output = {};
    var timestamp = new Date().getTime();
    output[timestamp] = payload;

    var offset = 6;
    var error = null;
    var _a, _b;
    for (var _i = 0, _c = rx[sensorType]; _i < _c.length; _i++) { // go through datastreams
        var dsName = _c[_i];
        var unit = rx_units[dsName];
        var len = (_a = unit.length) !== null && _a !== void 0 ? _a : 2;
        var value = readBytes(offset, len, bytes);
        if (unit.signed) {
            var complement = 1 << (len * 8 - 1);
            if (value > complement) {
                value -= complement * 2;
            }
        }
        var scalValue =
            Math.round(value * ((_b = unit.scale) !== null && _b !== void 0 ? _b : 1) * 10) / 10;
        var dsPayload = {}
        payload[dsName] = dsPayload;

        switch (sensorType) {
            case 12: //Twin-temp sensor kit
                if (scalValue > 400 || scalValue < -100) {
                    scalValue = null;
                    error = 'Sensor broken';
                }
                fillDatastream(dsPayload, error, scalValue);
                error = null;
                break;
            case 13: //4-20 mA input kit
                if (scalValue > 20.4 || scalValue < 3.6) {
                    scalValue = null;
                    error = 'Sensor broken';
                }
                fillDatastream(dsPayload, error, scalValue);
                error = null;
                break;
            default:
                break;
        }
        offset += len;
    }
    return { data: output };
}

function fillDatastream(dsPayload, error, value) {
    if (error != null) {
        if (dsPayload.e === undefined) {
            dsPayload.e = {};
        }
        dsPayload.e[error] = {}; // here only non-permanent errors are generated
    }
    if (value != null) {
        dsPayload.v = value;
    }
}


/**
 * Encode downlink function.
 * 
 * @param {object} input
 * @param {object} input.data Object representing the payload that must be encoded.
 * @param {Record<string, string>} input.variables Object containing the configured device variables.
 * 
 * @returns {{bytes: number[]}} Byte array containing the downlink payload.
 */
function encodeDownlink(input) {
    return {
        // !!!! Finish this function
        bytes: [0]
    };
}
